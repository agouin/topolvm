package e2e

import (
	_ "embed"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/types"
	. "github.com/onsi/gomega"
	"github.com/topolvm/topolvm"
	corev1 "k8s.io/api/core/v1"
)

func testThickProvisioning() {
	testNamespacePrefix := "thickptest-"
	var ns string
	var cc CleanupContext
	BeforeEach(func() {
		cc = commonBeforeEach()
		ns = testNamespacePrefix + randomString(10)
		createNamespace(ns)
	})

	AfterEach(func() {
		// When a test fails, I want to investigate the cause. So please don't remove the namespace!
		if !CurrentSpecReport().State.Is(types.SpecStateFailureStates) {
			kubectl("delete", "namespaces/"+ns)
		}
		commonAfterEach(cc)
	})

	It("should thick provision a PV", func() {
		By("deploying Pod with PVC")

		nodeName := "topolvm-e2e-worker"
		if isDaemonsetLvmdEnvSet() {
			nodeName = getDaemonsetLvmdNodeName()
		}

		thickPvcYAML := []byte(fmt.Sprintf(provPVCTemplateYAML, "thickvol", "1", thickStorageClassName))
		_, err := kubectlWithInput(thickPvcYAML, "apply", "-n", ns, "-f", "-")
		Expect(err).ShouldNot(HaveOccurred())

		thickPodYAML := []byte(fmt.Sprintf(podTemplateYAML, "thickpod", "thickvol", topolvm.GetTopologyNodeKey(), nodeName))
		_, err = kubectlWithInput(thickPodYAML, "apply", "-n", ns, "-f", "-")
		Expect(err).ShouldNot(HaveOccurred())

		By("confirming that the lv was created in the thick volume group")
		var lvName string
		Eventually(func() error {
			lvName, err = getLVNameOfPVC("thickvol", ns)
			return err
		}).Should(Succeed())

		var lv *lvinfo
		Eventually(func() error {
			lv, err = getLVInfo(lvName)
			return err
		}).Should(Succeed())

		vgName := "node-myvg1"
		Expect(vgName).Should(Equal(lv.vgName))

		By("deleting the Pod and PVC")
		_, err = kubectlWithInput(thickPodYAML, "delete", "-n", ns, "-f", "-")
		Expect(err).ShouldNot(HaveOccurred())
		_, err = kubectlWithInput(thickPvcYAML, "delete", "-n", ns, "-f", "-")
		Expect(err).ShouldNot(HaveOccurred())

		By("confirming that the PV is deleted")
		Eventually(func() error {
			var pv corev1.PersistentVolume
			err := getObjects(&pv, "pv", lvName)
			switch {
			case err == ErrObjectNotFound:
				return nil
			case err != nil:
				return fmt.Errorf("failed to get pv/%s. err: %w", lvName, err)
			default:
				return fmt.Errorf("target pv exists %s", lvName)
			}
		}).Should(Succeed())

		By("confirming that the lv correspond to LogicalVolume is deleted")
		Eventually(func() error {
			return checkLVIsDeletedInLVM(lvName)
		}).Should(Succeed())
	})
}
