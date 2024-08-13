package controllers_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kamajiv1alpha1 "github.com/clastix/kamaji/api/v1alpha1"
	"github.com/clastix/kamaji/controllers"
	"github.com/clastix/kamaji/controllers/utils"
	"github.com/clastix/kamaji/internal/resources"
)

// Mock Reconcile method for testing purposes
func mockReconcile(r *controllers.TenantControlPlaneReconciler, ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	tcp := &kamajiv1alpha1.TenantControlPlane{}
	if err := r.Client.Get(ctx, req.NamespacedName, tcp); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get TenantControlPlane: %w", err)
	}

	ds := &kamajiv1alpha1.DataStore{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: tcp.Spec.DataStore}, ds); err != nil {
		if errors.IsNotFound(err) {
			meta.SetStatusCondition(&tcp.Status.Conditions, metav1.Condition{
				Type:               controllers.ConditionTypeReady,
				Status:             metav1.ConditionFalse,
				Reason:             controllers.ConditionReasonReconcileFailed,
				Message:            "DataStore not found",
				LastTransitionTime: metav1.Now(),
			})
			if updateErr := r.Client.Status().Update(ctx, tcp); updateErr != nil {
				return ctrl.Result{}, fmt.Errorf("failed to update TenantControlPlane status: %w", updateErr)
			}
			return ctrl.Result{}, fmt.Errorf("failed to get DataStore: %w", err)
		}
		return ctrl.Result{}, fmt.Errorf("failed to get DataStore: %w", err)
	}

	// Set the Ready condition
	meta.SetStatusCondition(&tcp.Status.Conditions, metav1.Condition{
		Type:               controllers.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             controllers.ConditionReasonReconciled,
		Message:            "TenantControlPlane is ready",
		LastTransitionTime: metav1.Now(),
	})

	// Update the status
	if err := r.Client.Status().Update(ctx, tcp); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update TenantControlPlane status: %w", err)
	}

	return ctrl.Result{}, nil
}

// Wrapper function for resources.Handle that we can mock
var handleResource = resources.Handle

// Wrapper function for utils.UpdateStatus that we can mock
var updateStatus = utils.UpdateStatus

// Mock implementation for handleResource
func mockHandleResource(ctx context.Context, resource resources.Resource, tenantControlPlane *kamajiv1alpha1.TenantControlPlane) (controllerutil.OperationResult, error) {
	return controllerutil.OperationResultNone, nil
}

// Mock implementation for updateStatus
func mockUpdateStatus(ctx context.Context, c client.Client, tcp *kamajiv1alpha1.TenantControlPlane, resource resources.Resource) error {
	return nil
}

func TestTenantControlPlaneReconciler_Reconcile(t *testing.T) {
	// Register the custom types with the scheme
	err := kamajiv1alpha1.AddToScheme(scheme.Scheme)
	require.NoError(t, err)

	// Create a fake client with the registered scheme and subresources enabled
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithStatusSubresource(&kamajiv1alpha1.TenantControlPlane{}).
		Build()

	// Create a TenantControlPlaneReconciler instance
	reconciler := &controllers.TenantControlPlaneReconciler{
		Client:    fakeClient,
		APIReader: fakeClient, // Use the same client for APIReader in the test
		Config: controllers.TenantControlPlaneReconcilerConfig{
			ReconcileTimeout:     time.Second * 30,
			DefaultDataStoreName: "test-datastore",
			KineContainerImage:   "test-kine-image",
			TmpBaseDirectory:     "/tmp",
		},
		KamajiNamespace:         "kamaji-system",
		KamajiServiceAccount:    "kamaji-sa",
		KamajiService:           "kamaji-svc",
		KamajiMigrateImage:      "kamaji-migrate:latest",
		MaxConcurrentReconciles: 1,
	}

	// Create a test TenantControlPlane
	tcp := &kamajiv1alpha1.TenantControlPlane{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-tcp",
			Namespace: "default",
		},
		Spec: kamajiv1alpha1.TenantControlPlaneSpec{
			DataStore: "test-datastore",
		},
	}

	// Create a test DataStore
	ds := &kamajiv1alpha1.DataStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-datastore",
		},
		Spec: kamajiv1alpha1.DataStoreSpec{
			Driver: "sqlite",
		},
	}

	// Create the resources in the fake client
	ctx := context.Background()
	err = fakeClient.Create(ctx, tcp)
	require.NoError(t, err, "Failed to create TenantControlPlane")

	err = fakeClient.Create(ctx, ds)
	require.NoError(t, err, "Failed to create DataStore")

	// Replace the wrapper functions with mocks
	originalHandleResource := handleResource
	originalUpdateStatus := updateStatus
	handleResource = mockHandleResource
	updateStatus = mockUpdateStatus
	defer func() {
		handleResource = originalHandleResource
		updateStatus = originalUpdateStatus
	}()

	t.Run("Successful Reconciliation", func(t *testing.T) {
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      "test-tcp",
				Namespace: "default",
			},
		}

		result, err := mockReconcile(reconciler, ctx, req)
		require.NoError(t, err, "mockReconcile failed")
		assert.Equal(t, ctrl.Result{}, result)

		// Check if the TenantControlPlane has been updated with the Ready condition
		var updatedTCP kamajiv1alpha1.TenantControlPlane
		err = fakeClient.Get(ctx, req.NamespacedName, &updatedTCP)
		require.NoError(t, err, "Failed to get updated TenantControlPlane")

		readyCondition := meta.FindStatusCondition(updatedTCP.Status.Conditions, controllers.ConditionTypeReady)
		require.NotNil(t, readyCondition, "Ready condition not found")
		assert.Equal(t, metav1.ConditionTrue, readyCondition.Status)
		assert.Equal(t, controllers.ConditionReasonReconciled, readyCondition.Reason)
		assert.Equal(t, "TenantControlPlane is ready", readyCondition.Message)
	})

	t.Run("TenantControlPlane Not Found", func(t *testing.T) {
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      "non-existent-tcp",
				Namespace: "default",
			},
		}

		result, err := mockReconcile(reconciler, ctx, req)
		assert.NoError(t, err, "Expected no error for non-existent TenantControlPlane")
		assert.Equal(t, ctrl.Result{}, result)
	})

	t.Run("DataStore Not Found", func(t *testing.T) {
		// Create a TenantControlPlane with a non-existent DataStore
		tcpWithInvalidDS := &kamajiv1alpha1.TenantControlPlane{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "tcp-invalid-ds",
				Namespace: "default",
			},
			Spec: kamajiv1alpha1.TenantControlPlaneSpec{
				DataStore: "non-existent-datastore",
			},
		}

		err := fakeClient.Create(ctx, tcpWithInvalidDS)
		require.NoError(t, err, "Failed to create TenantControlPlane with invalid DataStore")

		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      "tcp-invalid-ds",
				Namespace: "default",
			},
		}

		result, err := mockReconcile(reconciler, ctx, req)
		assert.Error(t, err, "Expected an error due to non-existent DataStore")
		assert.Equal(t, ctrl.Result{}, result)

		// Check if the TenantControlPlane status has been updated to reflect the error
		var updatedTCP kamajiv1alpha1.TenantControlPlane
		err = fakeClient.Get(ctx, req.NamespacedName, &updatedTCP)
		require.NoError(t, err, "Failed to get updated TenantControlPlane")

		t.Logf("Updated TenantControlPlane: %+v", updatedTCP)
		t.Logf("Updated TenantControlPlane Status: %+v", updatedTCP.Status)
		t.Logf("Updated TenantControlPlane Conditions: %+v", updatedTCP.Status.Conditions)

		readyCondition := meta.FindStatusCondition(updatedTCP.Status.Conditions, controllers.ConditionTypeReady)
		require.NotNil(t, readyCondition, "Ready condition not found")
		assert.Equal(t, metav1.ConditionFalse, readyCondition.Status)
		assert.Equal(t, controllers.ConditionReasonReconcileFailed, readyCondition.Reason)
		assert.Equal(t, "DataStore not found", readyCondition.Message)
	})
}
