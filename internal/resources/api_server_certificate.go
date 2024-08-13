// Copyright 2022 Clastix Labs
// SPDX-License-Identifier: Apache-2.0

package resources

import (
	"context"
	"crypto/x509"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kamajiv1alpha1 "github.com/clastix/kamaji/api/v1alpha1"
	"github.com/clastix/kamaji/internal/constants"
	"github.com/clastix/kamaji/internal/crypto"
	"github.com/clastix/kamaji/internal/kubeadm"
	"github.com/clastix/kamaji/internal/utilities"
)

type APIServerCertificate struct {
	resource     *corev1.Secret
	Client       client.Client
	TmpDirectory string
}

func (r *APIServerCertificate) ShouldStatusBeUpdated(_ context.Context, tenantControlPlane *kamajiv1alpha1.TenantControlPlane) bool {
	return tenantControlPlane.Status.Certificates.APIServer.Checksum != utilities.GetObjectChecksum(r.resource)
}

func (r *APIServerCertificate) ShouldCleanup(_ *kamajiv1alpha1.TenantControlPlane) bool {
	return false
}

func (r *APIServerCertificate) CleanUp(context.Context, *kamajiv1alpha1.TenantControlPlane) (bool, error) {
	return false, nil
}

func (r *APIServerCertificate) Define(_ context.Context, tenantControlPlane *kamajiv1alpha1.TenantControlPlane) error {
	r.resource = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.getPrefixedName(tenantControlPlane),
			Namespace: tenantControlPlane.GetNamespace(),
		},
	}

	return nil
}

func (r *APIServerCertificate) getPrefixedName(tenantControlPlane *kamajiv1alpha1.TenantControlPlane) string {
	return utilities.AddTenantPrefix(r.GetName(), tenantControlPlane)
}

func (r *APIServerCertificate) GetClient() client.Client {
	return r.Client
}

func (r *APIServerCertificate) GetTmpDirectory() string {
	return r.TmpDirectory
}

func (r *APIServerCertificate) CreateOrUpdate(ctx context.Context, tenantControlPlane *kamajiv1alpha1.TenantControlPlane) (res controllerutil.OperationResult, err error) {
	return utilities.CreateOrUpdateWithConflict(ctx, r.Client, r.resource, r.mutate(ctx, tenantControlPlane))
}

func (r *APIServerCertificate) GetName() string {
	return "api-server-certificate"
}

func (r *APIServerCertificate) UpdateTenantControlPlaneStatus(_ context.Context, tenantControlPlane *kamajiv1alpha1.TenantControlPlane) error {
	tenantControlPlane.Status.Certificates.APIServer.LastUpdate = metav1.Now()
	tenantControlPlane.Status.Certificates.APIServer.SecretName = r.resource.GetName()
	tenantControlPlane.Status.Certificates.APIServer.Checksum = utilities.GetObjectChecksum(r.resource)

	return nil
}

func (r *APIServerCertificate) mutate(ctx context.Context, tenantControlPlane *kamajiv1alpha1.TenantControlPlane) controllerutil.MutateFn {
	return func() error {
		logger := log.FromContext(ctx, "resource", r.GetName())
		// Retrieving the TenantControlPlane CA:
		// this is required to trigger a new generation in case of Certificate Authority rotation.
		namespacedName := k8stypes.NamespacedName{Namespace: tenantControlPlane.GetNamespace(), Name: tenantControlPlane.Status.Certificates.CA.SecretName}
		secretCA := &corev1.Secret{}
		if err := r.Client.Get(ctx, namespacedName, secretCA); err != nil {
			logger.Error(err, "cannot retrieve CA secret")

			return err
		}

		r.resource.SetLabels(utilities.MergeMaps(
			utilities.KamajiLabels(tenantControlPlane.GetName(), r.GetName()),
			map[string]string{
				constants.ControllerLabelResource: "x509",
			},
		))

		if err := ctrl.SetControllerReference(tenantControlPlane, r.resource, r.Client.Scheme()); err != nil {
			logger.Error(err, "cannot set controller reference", "resource", r.GetName())

			return err
		}

		if checksum := tenantControlPlane.Status.Certificates.APIServer.Checksum; len(checksum) > 0 && checksum == utilities.GetObjectChecksum(r.resource) || len(r.resource.UID) > 0 {
			isCAValid, err := crypto.VerifyCertificate(r.resource.Data[kubeadmconstants.APIServerCertName], secretCA.Data[kubeadmconstants.CACertName], x509.ExtKeyUsageServerAuth)
			if err != nil {
				logger.Info(fmt.Sprintf("certificate-authority verify failed: %s", err.Error()))
			}

			isCertValid, err := crypto.CheckCertificateAndPrivateKeyPairValidity(
				r.resource.Data[kubeadmconstants.APIServerCertName],
				r.resource.Data[kubeadmconstants.APIServerKeyName],
			)
			if err != nil {
				logger.Info(fmt.Sprintf("%s certificate-private_key pair is not valid: %s", kubeadmconstants.APIServerCertAndKeyBaseName, err.Error()))
			}

			if isCAValid && isCertValid {
				return nil
			}
		}

		config, err := getStoredKubeadmConfiguration(ctx, r.Client, r.TmpDirectory, tenantControlPlane)
		if err != nil {
			logger.Error(err, "cannot retrieve kubeadm configuration")

			return err
		}

		ca := kubeadm.CertificatePrivateKeyPair{
			Name:        kubeadmconstants.CACertAndKeyBaseName,
			Certificate: secretCA.Data[kubeadmconstants.CACertName],
			PrivateKey:  secretCA.Data[kubeadmconstants.CAKeyName],
		}
		certificateKeyPair, err := kubeadm.GenerateCertificatePrivateKeyPair(kubeadmconstants.APIServerCertAndKeyBaseName, config, ca)
		if err != nil {
			logger.Error(err, "cannot generate certificate and private key in api server certificate", "details", err.Error())
			return fmt.Errorf("failed to generate certificate and private key: %w", err)
		}

		r.resource.Data = map[string][]byte{
			kubeadmconstants.APIServerCertName: certificateKeyPair.Certificate,
			kubeadmconstants.APIServerKeyName:  certificateKeyPair.PrivateKey,
		}

		utilities.SetObjectChecksum(r.resource, r.resource.Data)

		return nil
	}
}
