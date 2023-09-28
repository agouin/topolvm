package e2e

import (
	_ "embed"
	"errors"
	"fmt"
	"strconv"

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

		By("confirming that the lv was created in the thick volume group and pool")
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

		vgName := "node1-myvg4"
		if isDaemonsetLvmdEnvSet() {
			vgName = "node-myvg5"
		}
		Expect(vgName).Should(Equal(lv.vgName))

		poolName := "pool0"
		Expect(poolName).Should(Equal(lv.poolName))

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

	It("should overprovision thick PVCs", func() {
		By("deploying multiple PVCS with total size < thickpoolsize * overprovisioning")
		// The actual thickpool size is 4 GB . With an overprovisioning limit of 5, it should allow
		// PVCs totalling upto 20 GB for each node
		nodeName := "topolvm-e2e-worker2"
		if isDaemonsetLvmdEnvSet() {
			nodeName = getDaemonsetLvmdNodeName()
		}
		for i := 0; i < 5; i++ {
			num := strconv.Itoa(i)
			thickPvcYAML := []byte(fmt.Sprintf(provPVCTemplateYAML, "thickvol"+num, "3", thickStorageClassName))
			_, err := kubectlWithInput(thickPvcYAML, "apply", "-n", ns, "-f", "-")
			Expect(err).ShouldNot(HaveOccurred())

			thickPodYAML := []byte(fmt.Sprintf(podTemplateYAML, "thickpod"+num, "thickvol"+num, topolvm.GetTopologyNodeKey(), nodeName))
			_, err = kubectlWithInput(thickPodYAML, "apply", "-n", ns, "-f", "-")
			Expect(err).ShouldNot(HaveOccurred())

		}

		By("confirming that the volumes have been created in the thickpool")

		for i := 0; i < 5; i++ {
			var lvName string
			var err error

			num := strconv.Itoa(i)
			Eventually(func() error {
				lvName, err = getLVNameOfPVC("thickvol"+num, ns)
				return err
			}).Should(Succeed())

			var lv *lvinfo
			Eventually(func() error {
				lv, err = getLVInfo(lvName)
				return err
			}).Should(Succeed())

			vgName := "node2-myvg4"
			if isDaemonsetLvmdEnvSet() {
				vgName = "node-myvg5"
			}
			Expect(vgName).Should(Equal(lv.vgName))

			poolName := "pool0"
			Expect(poolName).Should(Equal(lv.poolName))
		}

		By("deleting the Pods and PVCs")

		for i := 0; i < 5; i++ {
			num := strconv.Itoa(i)
			_, err := kubectl("delete", "-n", ns, "pod", "thickpod"+num)
			Expect(err).ShouldNot(HaveOccurred())
			_, err = kubectl("delete", "-n", ns, "pvc", "thickvol"+num)
			Expect(err).ShouldNot(HaveOccurred())

			By("confirming the Pod is deleted")
			Eventually(func() error {
				var pod corev1.Pod
				err := getObjects(&pod, "pod", "-n", ns, "thickpod"+num)
				switch {
				case err == ErrObjectNotFound:
					return nil
				case err != nil:
					return err
				default:
					return errors.New("the Pod exists")
				}
			}).Should(Succeed())

			By("confirming the PVC is deleted")
			Eventually(func() error {
				var pvc corev1.PersistentVolumeClaim
				err := getObjects(&pvc, "pvc", "-n", ns, "thickvol"+num)
				switch {
				case err == ErrObjectNotFound:
					return nil
				case err != nil:
					return err
				default:
					return errors.New("the PVC exists")
				}
			}).Should(Succeed())
		}
	})

	It("should check overprovision limits", func() {
		By("Deploying a PVC to use up the available thickpoolsize * overprovisioning")

		nodeName := "topolvm-e2e-worker3"
		if isDaemonsetLvmdEnvSet() {
			nodeName = getDaemonsetLvmdNodeName()
		}

		thickPvcYAML := []byte(fmt.Sprintf(provPVCTemplateYAML, "thickvol", "18", thickStorageClassName))
		_, err := kubectlWithInput(thickPvcYAML, "apply", "-n", ns, "-f", "-")
		Expect(err).ShouldNot(HaveOccurred())

		thickPodYAML := []byte(fmt.Sprintf(podTemplateYAML, "thickpod", "thickvol", topolvm.GetTopologyNodeKey(), nodeName))
		_, err = kubectlWithInput(thickPodYAML, "apply", "-n", ns, "-f", "-")
		Expect(err).ShouldNot(HaveOccurred())

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

		vgName := "node3-myvg4"
		if isDaemonsetLvmdEnvSet() {
			vgName = "node-myvg5"
		}
		Expect(vgName).Should(Equal(lv.vgName))

		poolName := "pool0"
		Expect(poolName).Should(Equal(lv.poolName))

		By("Failing to deploying a PVC when total size > thickpoolsize * overprovisioning")
		thickPvcYAML = []byte(fmt.Sprintf(provPVCTemplateYAML, "thickvol2", "5", thickStorageClassName))
		_, err = kubectlWithInput(thickPvcYAML, "apply", "-n", ns, "-f", "-")
		Expect(err).ShouldNot(HaveOccurred())

		thickPodYAML = []byte(fmt.Sprintf(podTemplateYAML, "thickpod2", "thickvol2", topolvm.GetTopologyNodeKey(), nodeName))
		_, err = kubectlWithInput(thickPodYAML, "apply", "-n", ns, "-f", "-")
		Expect(err).ShouldNot(HaveOccurred())

		Eventually(func() error {
			var pvc corev1.PersistentVolumeClaim
			err = getObjects(&pvc, "pvc", "-n", ns, "thickvol2")
			if err != nil {
				return fmt.Errorf("failed to get PVC. err: %w", err)
			}
			if pvc.Status.Phase == corev1.ClaimBound {
				return fmt.Errorf("PVC should not be bound")
			}
			return nil
		}).Should(Succeed())

		By("Deleting the pods and pvcs")
		_, err = kubectl("delete", "-n", ns, "pod", "thickpod")
		Expect(err).ShouldNot(HaveOccurred())

		_, err = kubectl("delete", "-n", ns, "pod", "thickpod2")
		Expect(err).ShouldNot(HaveOccurred())

		_, err = kubectl("delete", "-n", ns, "pvc", "thickvol")
		Expect(err).ShouldNot(HaveOccurred())

		_, err = kubectl("delete", "-n", ns, "pvc", "thickvol2")
		Expect(err).ShouldNot(HaveOccurred())

		By("confirming the Pods are deleted")
		Eventually(func() error {
			var pod corev1.Pod
			err := getObjects(&pod, "pod", "-n", ns, "thickpod")
			switch {
			case err == ErrObjectNotFound:
				return nil
			case err != nil:
				return err
			default:
				return errors.New("the Pod exists")
			}
		}).Should(Succeed())

		Eventually(func() error {
			var pod corev1.Pod
			err := getObjects(&pod, "pod", "-n", ns, "thickpod2")
			switch {
			case err == ErrObjectNotFound:
				return nil
			case err != nil:
				return err
			default:
				return errors.New("the Pod exists")
			}
		}).Should(Succeed())

		By("confirming the PVCs are deleted")
		Eventually(func() error {
			var pvc corev1.PersistentVolumeClaim
			err := getObjects(&pvc, "pvc", "-n", ns, "thickvol")
			switch {
			case err == ErrObjectNotFound:
				return nil
			case err != nil:
				return err
			default:
				return errors.New("the PVC exists")
			}
		}).Should(Succeed())

		Eventually(func() error {
			var pvc corev1.PersistentVolumeClaim
			err := getObjects(&pvc, "pvc", "-n", ns, "thickvol2")
			switch {
			case err == ErrObjectNotFound:
				return nil
			case err != nil:
				return err
			default:
				return errors.New("the PVC exists")
			}
		}).Should(Succeed())
	})
}
