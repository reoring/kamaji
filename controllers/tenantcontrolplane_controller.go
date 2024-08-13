// Copyright 2022 Clastix Labs
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/juju/mutex/v2"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	kamajiv1alpha1 "github.com/clastix/kamaji/api/v1alpha1"
	"github.com/clastix/kamaji/controllers/finalizers"
	"github.com/clastix/kamaji/controllers/utils"
	"github.com/clastix/kamaji/internal/datastore"
	kamajierrors "github.com/clastix/kamaji/internal/errors"
	"github.com/clastix/kamaji/internal/resources"
)

// Constants for condition types and reasons
const (
	ConditionTypeReady              = "Ready"
	ConditionReasonRetrievalFailed  = "RetrievalFailed"
	ConditionReasonReconcilePending = "ReconciliationPending"
	ConditionReasonReconcileFailed  = "ReconciliationFailed"
	ConditionReasonReconciled       = "Reconciled"
)

// TenantControlPlaneReconciler reconciles a TenantControlPlane object.
type TenantControlPlaneReconciler struct {
	Client                  client.Client
	APIReader               client.Reader
	Config                  TenantControlPlaneReconcilerConfig
	TriggerChan             TenantControlPlaneChannel
	KamajiNamespace         string
	KamajiServiceAccount    string
	KamajiService           string
	KamajiMigrateImage      string
	MaxConcurrentReconciles int
	// CertificateChan is the channel used by the CertificateLifecycleController that is checking for
	// certificates and kubeconfig user certs validity: a generic event for the given TCP will be triggered
	// once the validity threshold for the given certificate is reached.
	CertificateChan CertificateChannel

	clock mutex.Clock
}

// TenantControlPlaneReconcilerConfig gives the necessary configuration for TenantControlPlaneReconciler.
type TenantControlPlaneReconcilerConfig struct {
	ReconcileTimeout     time.Duration
	DefaultDataStoreName string
	KineContainerImage   string
	TmpBaseDirectory     string
}

//+kubebuilder:rbac:groups=kamaji.clastix.io,resources=tenantcontrolplanes,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=kamaji.clastix.io,resources=tenantcontrolplanes/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=kamaji.clastix.io,resources=tenantcontrolplanes/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete

func (r *TenantControlPlaneReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var cancelFn context.CancelFunc
	ctx, cancelFn = context.WithTimeout(ctx, r.Config.ReconcileTimeout)
	defer cancelFn()

	tenantControlPlane, err := r.getTenantControlPlane(ctx, req.NamespacedName)()
	if err != nil {
		return r.handleTenantControlPlaneRetrievalError(ctx, log, err)
	}

	releaser, err := r.acquireMutex(tenantControlPlane)
	if err != nil {
		return r.handleMutexAcquisitionError(log, err)
	}
	defer releaser.Release()

	if r.shouldSkipReconciliation(tenantControlPlane) {
		return ctrl.Result{}, nil
	}

	ds, err := r.getDataStore(ctx, tenantControlPlane)
	if err != nil {
		log.Error(err, "cannot retrieve the DataStore for the given instance")
		return ctrl.Result{}, err
	}

	dsConnection, err := r.createDataStoreConnection(ctx, ds)
	if err != nil {
		log.Error(err, "cannot generate the DataStore connection for the given instance")
		return ctrl.Result{}, err
	}
	defer dsConnection.Close()

	if tenantControlPlane.GetDeletionTimestamp() != nil {
		return r.handleDeletion(ctx, log, tenantControlPlane, ds, dsConnection)
	}

	return r.handleReconciliation(ctx, log, tenantControlPlane, ds, dsConnection)
}

// handleTenantControlPlaneRetrievalError handles errors when retrieving the TenantControlPlane.
func (r *TenantControlPlaneReconciler) handleTenantControlPlaneRetrievalError(ctx context.Context, log logr.Logger, err error) (ctrl.Result, error) {
	if k8serrors.IsNotFound(err) {
		log.Info("resource has been deleted, skipping")
		return reconcile.Result{}, nil
	}
	log.Error(err, "cannot retrieve the required resource")
	return ctrl.Result{}, err
}

// acquireMutex attempts to acquire a mutex for the given TenantControlPlane.
func (r *TenantControlPlaneReconciler) acquireMutex(tenantControlPlane *kamajiv1alpha1.TenantControlPlane) (mutex.Releaser, error) {
	return mutex.Acquire(r.mutexSpec(tenantControlPlane))
}

// handleMutexAcquisitionError handles errors when acquiring the mutex.
func (r *TenantControlPlaneReconciler) handleMutexAcquisitionError(log logr.Logger, err error) (ctrl.Result, error) {
	switch {
	case errors.As(err, &mutex.ErrTimeout):
		log.Info("acquire timed out, current process is blocked by another reconciliation")
		return ctrl.Result{Requeue: true}, nil
	case errors.As(err, &mutex.ErrCancelled):
		log.Info("acquire cancelled")
		return ctrl.Result{Requeue: true}, nil
	default:
		log.Error(err, "acquire failed")
		return ctrl.Result{}, err
	}
}

// shouldSkipReconciliation determines if reconciliation should be skipped.
func (r *TenantControlPlaneReconciler) shouldSkipReconciliation(tenantControlPlane *kamajiv1alpha1.TenantControlPlane) bool {
	return tenantControlPlane.GetDeletionTimestamp() != nil && !controllerutil.ContainsFinalizer(tenantControlPlane, finalizers.DatastoreFinalizer)
}

// getDataStore retrieves the DataStore for the given TenantControlPlane.
func (r *TenantControlPlaneReconciler) getDataStore(ctx context.Context, tenantControlPlane *kamajiv1alpha1.TenantControlPlane) (*kamajiv1alpha1.DataStore, error) {
	dataStoreName := tenantControlPlane.Spec.DataStore
	if len(dataStoreName) == 0 {
		dataStoreName = r.Config.DefaultDataStoreName
	}

	ds := &kamajiv1alpha1.DataStore{}
	if err := r.Client.Get(ctx, k8stypes.NamespacedName{Name: dataStoreName}, ds); err != nil {
		return nil, errors.Wrap(err, "cannot retrieve *kamajiv1alpha.DataStore object")
	}

	return ds, nil
}

// createDataStoreConnection creates a connection to the given DataStore.
func (r *TenantControlPlaneReconciler) createDataStoreConnection(ctx context.Context, ds *kamajiv1alpha1.DataStore) (datastore.Connection, error) {
	return datastore.NewStorageConnection(ctx, r.Client, *ds)
}

// handleDeletion handles the deletion of a TenantControlPlane.
func (r *TenantControlPlaneReconciler) handleDeletion(ctx context.Context, log logr.Logger, tenantControlPlane *kamajiv1alpha1.TenantControlPlane, ds *kamajiv1alpha1.DataStore, dsConnection datastore.Connection) (ctrl.Result, error) {
	log.Info("marked for deletion, performing clean-up")

	groupDeletableResourceBuilderConfiguration := GroupDeletableResourceBuilderConfiguration{
		client:              r.Client,
		log:                 log,
		tcpReconcilerConfig: r.Config,
		tenantControlPlane:  *tenantControlPlane,
		connection:          dsConnection,
		dataStore:           *ds,
	}

	for _, resource := range GetDeletableResources(tenantControlPlane, groupDeletableResourceBuilderConfiguration) {
		if err := resources.HandleDeletion(ctx, resource, tenantControlPlane); err != nil {
			log.Error(err, "resource deletion failed", "resource", resource.GetName())
			return ctrl.Result{}, err
		}
	}

	log.Info("resource deletions have been completed")
	return ctrl.Result{}, nil
}

// handleReconciliation handles the reconciliation of a TenantControlPlane.
func (r *TenantControlPlaneReconciler) handleReconciliation(ctx context.Context, log logr.Logger, tenantControlPlane *kamajiv1alpha1.TenantControlPlane, ds *kamajiv1alpha1.DataStore, dsConnection datastore.Connection) (ctrl.Result, error) {
	groupResourceBuilderConfiguration := GroupResourceBuilderConfiguration{
		client:               r.Client,
		log:                  log,
		tcpReconcilerConfig:  r.Config,
		tenantControlPlane:   *tenantControlPlane,
		Connection:           dsConnection,
		DataStore:            *ds,
		KamajiNamespace:      r.KamajiNamespace,
		KamajiServiceAccount: r.KamajiServiceAccount,
		KamajiService:        r.KamajiService,
		KamajiMigrateImage:   r.KamajiMigrateImage,
	}
	registeredResources := GetResources(groupResourceBuilderConfiguration)

	for _, resource := range registeredResources {
		result, err := resources.Handle(ctx, resource, tenantControlPlane)
		if err != nil {
			return r.handleResourceError(ctx, log, tenantControlPlane, resource, err)
		}

		if result == controllerutil.OperationResultNone {
			continue
		}

		if err = utils.UpdateStatus(ctx, r.Client, tenantControlPlane, resource); err != nil {
			log.Error(err, "update of the resource failed", "resource", resource.GetName())
			return ctrl.Result{}, err
		}

		log.Info(fmt.Sprintf("%s has been configured", resource.GetName()))

		if result == resources.OperationResultEnqueueBack {
			log.Info("requested enqueuing back", "resources", resource.GetName())
			return ctrl.Result{Requeue: true}, nil
		}
	}

	log.Info(fmt.Sprintf("%s has been reconciled", tenantControlPlane.GetName()))

	tenantControlPlane.SetCondition(ConditionTypeReady, metav1.ConditionTrue, ConditionReasonReconciled, "TenantControlPlane is ready")

	if err := r.Client.Status().Update(ctx, tenantControlPlane); err != nil {
		log.Error(err, "failed to update TenantControlPlane status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// handleResourceError handles errors that occur during resource handling.
func (r *TenantControlPlaneReconciler) handleResourceError(ctx context.Context, log logr.Logger, tenantControlPlane *kamajiv1alpha1.TenantControlPlane, resource resources.Resource, err error) (ctrl.Result, error) {
	if kamajierrors.ShouldReconcileErrorBeIgnored(err) {
		log.V(1).Info("sentinel error, enqueuing back request", "error", err.Error())
		tenantControlPlane.SetCondition(ConditionTypeReady, metav1.ConditionFalse, ConditionReasonReconcilePending, "Reconciliation temporarily failed, will retry")
		if updateErr := r.Client.Status().Update(ctx, tenantControlPlane); updateErr != nil {
			log.Error(updateErr, "failed to update TenantControlPlane status")
		}
		return ctrl.Result{Requeue: true}, nil
	}

	log.Error(err, "handling of resource failed", "resource", resource.GetName())
	tenantControlPlane.SetCondition(ConditionTypeReady, metav1.ConditionFalse, ConditionReasonReconcileFailed, fmt.Sprintf("Failed to reconcile resource: %s", resource.GetName()))
	if updateErr := r.Client.Status().Update(ctx, tenantControlPlane); updateErr != nil {
		log.Error(updateErr, "failed to update TenantControlPlane status")
	}
	return ctrl.Result{}, err
}

func (r *TenantControlPlaneReconciler) mutexSpec(obj client.Object) mutex.Spec {
	return mutex.Spec{
		Name:    strings.ReplaceAll(fmt.Sprintf("kamaji%s", obj.GetUID()), "-", ""),
		Clock:   r.clock,
		Delay:   10 * time.Millisecond,
		Timeout: time.Second,
		Cancel:  nil,
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *TenantControlPlaneReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.clock = clock.RealClock{}

	return ctrl.NewControllerManagedBy(mgr).
		WatchesRawSource(source.Channel(r.CertificateChan, handler.Funcs{GenericFunc: func(_ context.Context, genericEvent event.GenericEvent, limitingInterface workqueue.RateLimitingInterface) {
			limitingInterface.AddRateLimited(ctrl.Request{
				NamespacedName: k8stypes.NamespacedName{
					Namespace: genericEvent.Object.GetNamespace(),
					Name:      genericEvent.Object.GetName(),
				},
			})
		}})).
		WatchesRawSource(source.Channel(r.TriggerChan, handler.Funcs{GenericFunc: func(_ context.Context, genericEvent event.GenericEvent, limitingInterface workqueue.RateLimitingInterface) {
			limitingInterface.AddRateLimited(ctrl.Request{
				NamespacedName: k8stypes.NamespacedName{
					Namespace: genericEvent.Object.GetNamespace(),
					Name:      genericEvent.Object.GetName(),
				},
			})
		}})).
		For(&kamajiv1alpha1.TenantControlPlane{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&networkingv1.Ingress{}).
		Watches(&batchv1.Job{}, handler.EnqueueRequestsFromMapFunc(func(_ context.Context, object client.Object) []reconcile.Request {
			labels := object.GetLabels()

			name, namespace := labels["tcp.kamaji.clastix.io/name"], labels["tcp.kamaji.clastix.io/namespace"]

			return []reconcile.Request{
				{
					NamespacedName: k8stypes.NamespacedName{
						Namespace: namespace,
						Name:      name,
					},
				},
			}
		}), builder.WithPredicates(predicate.NewPredicateFuncs(func(object client.Object) bool {
			if object.GetNamespace() != r.KamajiNamespace {
				return false
			}

			labels := object.GetLabels()

			if labels == nil {
				return false
			}

			v, ok := labels["kamaji.clastix.io/component"]

			return ok && v == "migrate"
		}))).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrentReconciles,
		}).
		Complete(r)
}

func (r *TenantControlPlaneReconciler) getTenantControlPlane(ctx context.Context, namespacedName k8stypes.NamespacedName) utils.TenantControlPlaneRetrievalFn {
	return func() (*kamajiv1alpha1.TenantControlPlane, error) {
		tcp := &kamajiv1alpha1.TenantControlPlane{}
		if err := r.APIReader.Get(ctx, namespacedName, tcp); err != nil {
			return nil, err
		}

		return tcp, nil
	}
}

func (r *TenantControlPlaneReconciler) RemoveFinalizer(ctx context.Context, tenantControlPlane *kamajiv1alpha1.TenantControlPlane) error {
	controllerutil.RemoveFinalizer(tenantControlPlane, finalizers.DatastoreFinalizer)

	return r.Client.Update(ctx, tenantControlPlane)
}
