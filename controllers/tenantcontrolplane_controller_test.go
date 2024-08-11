package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kamajiv1alpha1 "github.com/clastix/kamaji/api/v1alpha1"
)

func TestTenantControlPlaneReconciler_Reconcile(t *testing.T) {
	// Create a fake client
	client := fake.NewClientBuilder().Build()

	// Create a TenantControlPlaneReconciler instance
	reconciler := &TenantControlPlaneReconciler{
		Client: client,
		Config: TenantControlPlaneReconcilerConfig{
			ReconcileTimeout:     time.Second * 30,
			DefaultDataStoreName: "test-datastore",
		},
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
	err := client.Create(context.Background(), tcp)
	require.NoError(t, err)

	err = client.Create(context.Background(), ds)
	require.NoError(t, err)

	// Call Reconcile
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-tcp",
			Namespace: "default",
		},
	}

	result, err := reconciler.Reconcile(context.Background(), req)

	// Assert the result
	assert.NoError(t, err)
	assert.Equal(t, reconcile.Result{}, result)

	// Verify the TenantControlPlane status
	var updatedTCP kamajiv1alpha1.TenantControlPlane
	err = client.Get(context.Background(), req.NamespacedName, &updatedTCP)
	require.NoError(t, err)

	readyCondition := updatedTCP.GetCondition("Ready")
	assert.NotNil(t, readyCondition)
	assert.Equal(t, metav1.ConditionTrue, readyCondition.Status)
	assert.Equal(t, "Reconciled", readyCondition.Reason)
	assert.Equal(t, "TenantControlPlane is ready", readyCondition.Message)
}
