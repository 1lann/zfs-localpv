/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package zfs

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/openebs/lib-csi/pkg/btrfs"
	"github.com/openebs/lib-csi/pkg/xfs"
	apis "github.com/openebs/zfs-localpv/pkg/apis/openebs.io/zfs/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog"
)

// zfs related constants
const (
	ZFSDevPath = "/dev/zvol/"
	FSTypeZFS  = "zfs"
)

// zfs command related constants
const (
	ZFSVolCmd      = "zfs"
	ZFSCreateArg   = "create"
	ZFSCloneArg    = "clone"
	ZFSDestroyArg  = "destroy"
	ZFSSetArg      = "set"
	ZFSGetArg      = "get"
	ZFSListArg     = "list"
	ZFSSnapshotArg = "snapshot"
	ZFSSendArg     = "send"
	ZFSRecvArg     = "recv"
)

// constants to define volume type
const (
	VolTypeDataset = "DATASET"
	VolTypeZVol    = "ZVOL"
)

// PropertyChanged return whether volume property is changed
func PropertyChanged(oldVol *apis.ZFSVolume, newVol *apis.ZFSVolume) bool {
	if oldVol.Spec.VolumeType == VolTypeDataset &&
		newVol.Spec.VolumeType == VolTypeDataset &&
		oldVol.Spec.RecordSize != newVol.Spec.RecordSize {
		return true
	}

	return oldVol.Spec.Compression != newVol.Spec.Compression ||
		oldVol.Spec.Dedup != newVol.Spec.Dedup
}

// GetVolumeType returns the volume type
// whether it is a zvol or dataset
func GetVolumeType(fstype string) string {
	/*
	 * if fstype is provided as zfs then a zfs dataset will be created
	 * otherwise a zvol will be created
	 */
	switch fstype {
	case FSTypeZFS:
		return VolTypeDataset
	default:
		return VolTypeZVol
	}
}

// builldZvolCreateArgs returns zfs create command for zvol along with attributes as a string array
func buildZvolCreateArgs(vol *apis.ZFSVolume) []string {
	var zfsVolArgs []string

	volume := vol.Spec.PoolName + "/" + vol.Name

	zfsVolArgs = append(zfsVolArgs, ZFSCreateArg)

	if vol.Spec.ThinProvision == "yes" {
		zfsVolArgs = append(zfsVolArgs, "-s")
	}
	if len(vol.Spec.Capacity) != 0 {
		zfsVolArgs = append(zfsVolArgs, "-V", vol.Spec.Capacity)
	}
	if len(vol.Spec.VolBlockSize) != 0 {
		zfsVolArgs = append(zfsVolArgs, "-b", vol.Spec.VolBlockSize)
	}
	if len(vol.Spec.Dedup) != 0 {
		dedupProperty := "dedup=" + vol.Spec.Dedup
		zfsVolArgs = append(zfsVolArgs, "-o", dedupProperty)
	}
	if len(vol.Spec.Compression) != 0 {
		compressionProperty := "compression=" + vol.Spec.Compression
		zfsVolArgs = append(zfsVolArgs, "-o", compressionProperty)
	}
	if len(vol.Spec.Encryption) != 0 {
		encryptionProperty := "encryption=" + vol.Spec.Encryption
		zfsVolArgs = append(zfsVolArgs, "-o", encryptionProperty)
	}
	if len(vol.Spec.KeyLocation) != 0 {
		keyLocation := "keylocation=" + vol.Spec.KeyLocation
		zfsVolArgs = append(zfsVolArgs, "-o", keyLocation)
	}
	if len(vol.Spec.KeyFormat) != 0 {
		keyFormat := "keyformat=" + vol.Spec.KeyFormat
		zfsVolArgs = append(zfsVolArgs, "-o", keyFormat)
	}

	zfsVolArgs = append(zfsVolArgs, volume)

	return zfsVolArgs
}

// builldCloneCreateArgs returns zfs clone commands for zfs volume/dataset along with attributes as a string array
func buildCloneCreateArgs(vol *apis.ZFSVolume) []string {
	var zfsVolArgs []string

	volume := vol.Spec.PoolName + "/" + vol.Name
	snapshot := vol.Spec.PoolName + "/" + vol.Spec.SnapName

	zfsVolArgs = append(zfsVolArgs, ZFSCloneArg)

	if vol.Spec.VolumeType == VolTypeDataset {
		if len(vol.Spec.Capacity) != 0 {
			quotaProperty := "quota=" + vol.Spec.Capacity
			zfsVolArgs = append(zfsVolArgs, "-o", quotaProperty)
		}
		if len(vol.Spec.RecordSize) != 0 {
			recordsizeProperty := "recordsize=" + vol.Spec.RecordSize
			zfsVolArgs = append(zfsVolArgs, "-o", recordsizeProperty)
		}
		if vol.Spec.ThinProvision == "no" {
			reservationProperty := "reservation=" + vol.Spec.Capacity
			zfsVolArgs = append(zfsVolArgs, "-o", reservationProperty)
		}
		zfsVolArgs = append(zfsVolArgs, "-o", "mountpoint=legacy")
	}

	if len(vol.Spec.Dedup) != 0 {
		dedupProperty := "dedup=" + vol.Spec.Dedup
		zfsVolArgs = append(zfsVolArgs, "-o", dedupProperty)
	}
	if len(vol.Spec.Compression) != 0 {
		compressionProperty := "compression=" + vol.Spec.Compression
		zfsVolArgs = append(zfsVolArgs, "-o", compressionProperty)
	}
	if len(vol.Spec.Encryption) != 0 {
		encryptionProperty := "encryption=" + vol.Spec.Encryption
		zfsVolArgs = append(zfsVolArgs, "-o", encryptionProperty)
	}
	if len(vol.Spec.KeyLocation) != 0 {
		keyLocation := "keylocation=" + vol.Spec.KeyLocation
		zfsVolArgs = append(zfsVolArgs, "-o", keyLocation)
	}
	if len(vol.Spec.KeyFormat) != 0 {
		keyFormat := "keyformat=" + vol.Spec.KeyFormat
		zfsVolArgs = append(zfsVolArgs, "-o", keyFormat)
	}
	zfsVolArgs = append(zfsVolArgs, snapshot, volume)
	return zfsVolArgs
}

// buildZFSSnapCreateArgs returns zfs create command for zfs snapshot
// zfs snapshot <poolname>/<volname>@<snapname>
func buildZFSSnapCreateArgs(snap *apis.ZFSSnapshot) []string {
	var ZFSSnapArg []string

	volname := snap.Labels[ZFSVolKey]
	snapDataset := snap.Spec.PoolName + "/" + volname + "@" + snap.Name

	ZFSSnapArg = append(ZFSSnapArg, ZFSSnapshotArg, snapDataset)

	return ZFSSnapArg
}

// builldZFSSnapDestroyArgs returns zfs destroy command for zfs snapshot
// zfs destroy <poolname>/<volname>@<snapname>
func buildZFSSnapDestroyArgs(snap *apis.ZFSSnapshot) []string {
	var ZFSSnapArg []string

	volname := snap.Labels[ZFSVolKey]
	snapDataset := snap.Spec.PoolName + "/" + volname + "@" + snap.Name

	ZFSSnapArg = append(ZFSSnapArg, ZFSDestroyArg, snapDataset)

	return ZFSSnapArg
}

// builldDatasetCreateArgs returns zfs create command for dataset along with attributes as a string array
func buildDatasetCreateArgs(vol *apis.ZFSVolume) []string {
	var zfsVolArgs []string

	volume := vol.Spec.PoolName + "/" + vol.Name

	zfsVolArgs = append(zfsVolArgs, ZFSCreateArg)

	if len(vol.Spec.Capacity) != 0 {
		quotaProperty := "quota=" + vol.Spec.Capacity
		zfsVolArgs = append(zfsVolArgs, "-o", quotaProperty)
	}
	if len(vol.Spec.RecordSize) != 0 {
		recordsizeProperty := "recordsize=" + vol.Spec.RecordSize
		zfsVolArgs = append(zfsVolArgs, "-o", recordsizeProperty)
	}
	if vol.Spec.ThinProvision == "no" {
		reservationProperty := "reservation=" + vol.Spec.Capacity
		zfsVolArgs = append(zfsVolArgs, "-o", reservationProperty)
	}
	if len(vol.Spec.Dedup) != 0 {
		dedupProperty := "dedup=" + vol.Spec.Dedup
		zfsVolArgs = append(zfsVolArgs, "-o", dedupProperty)
	}
	if len(vol.Spec.Compression) != 0 {
		compressionProperty := "compression=" + vol.Spec.Compression
		zfsVolArgs = append(zfsVolArgs, "-o", compressionProperty)
	}
	if len(vol.Spec.Encryption) != 0 {
		encryptionProperty := "encryption=" + vol.Spec.Encryption
		zfsVolArgs = append(zfsVolArgs, "-o", encryptionProperty)
	}
	if len(vol.Spec.KeyLocation) != 0 {
		keyLocation := "keylocation=" + vol.Spec.KeyLocation
		zfsVolArgs = append(zfsVolArgs, "-o", keyLocation)
	}
	if len(vol.Spec.KeyFormat) != 0 {
		keyFormat := "keyformat=" + vol.Spec.KeyFormat
		zfsVolArgs = append(zfsVolArgs, "-o", keyFormat)
	}

	// set the mount path to none, by default zfs mounts it to the default dataset path
	zfsVolArgs = append(zfsVolArgs, "-o", "mountpoint=legacy", volume)

	return zfsVolArgs
}

// builldVolumeSetArgs returns volume set command along with attributes as a string array
// TODO(pawan) need to find a way to identify which property has changed
func buildVolumeSetArgs(vol *apis.ZFSVolume) []string {
	var zfsVolArgs []string

	volume := vol.Spec.PoolName + "/" + vol.Name

	zfsVolArgs = append(zfsVolArgs, ZFSSetArg)

	if vol.Spec.VolumeType == VolTypeDataset &&
		len(vol.Spec.RecordSize) != 0 {
		recordsizeProperty := "recordsize=" + vol.Spec.RecordSize
		zfsVolArgs = append(zfsVolArgs, recordsizeProperty)
	}

	if len(vol.Spec.Dedup) != 0 {
		dedupProperty := "dedup=" + vol.Spec.Dedup
		zfsVolArgs = append(zfsVolArgs, dedupProperty)
	}
	if len(vol.Spec.Compression) != 0 {
		compressionProperty := "compression=" + vol.Spec.Compression
		zfsVolArgs = append(zfsVolArgs, compressionProperty)
	}

	zfsVolArgs = append(zfsVolArgs, volume)

	return zfsVolArgs
}

// builldVolumeResizeArgs returns volume set  for resizing the zfs volume
func buildVolumeResizeArgs(vol *apis.ZFSVolume) []string {
	var zfsVolArgs []string

	volume := vol.Spec.PoolName + "/" + vol.Name

	zfsVolArgs = append(zfsVolArgs, ZFSSetArg)

	if vol.Spec.VolumeType == VolTypeDataset {
		quotaProperty := "quota=" + vol.Spec.Capacity
		zfsVolArgs = append(zfsVolArgs, quotaProperty)
	} else {
		volsizeProperty := "volsize=" + vol.Spec.Capacity
		zfsVolArgs = append(zfsVolArgs, volsizeProperty)
	}

	zfsVolArgs = append(zfsVolArgs, volume)

	return zfsVolArgs
}

// builldVolumeBackupArgs returns volume send command for sending the zfs volume
func buildVolumeBackupArgs(bkp *apis.ZFSBackup, vol *apis.ZFSVolume) []string {
	zfsVolArgs := []string{ZFSSendArg}

	curSnap := vol.Spec.PoolName + "/" + vol.Name + "@" + bkp.Spec.SnapName

	if len(bkp.Spec.PrevSnapName) > 0 {
		// do incremental send
		prevSnap := vol.Spec.PoolName + "/" + vol.Name + "@" + bkp.Spec.PrevSnapName
		zfsVolArgs = append(zfsVolArgs, "-i", prevSnap, curSnap)
	} else {
		zfsVolArgs = append(zfsVolArgs, curSnap)
	}

	return zfsVolArgs
}

// builldVolumeRestoreArgs returns volume recv command for receiving the zfs volume
func buildVolumeRestoreArgs(rstr *apis.ZFSRestore) []string {
	zfsVolArgs := []string{ZFSRecvArg}

	volume := rstr.VolSpec.PoolName + "/" + rstr.Spec.VolumeName

	if rstr.VolSpec.VolumeType == VolTypeDataset {
		if len(rstr.VolSpec.Capacity) != 0 {
			zfsVolArgs = append(zfsVolArgs, "-o", "quota="+rstr.VolSpec.Capacity)
		}
		if len(rstr.VolSpec.RecordSize) != 0 {
			zfsVolArgs = append(zfsVolArgs, "-o", "recordsize="+rstr.VolSpec.RecordSize)
		}
		if rstr.VolSpec.ThinProvision == "no" {
			zfsVolArgs = append(zfsVolArgs, "-o", "reservation="+rstr.VolSpec.Capacity)
		}
		zfsVolArgs = append(zfsVolArgs, "-o", "mountpoint=legacy")
	}

	if len(rstr.VolSpec.Dedup) != 0 {
		zfsVolArgs = append(zfsVolArgs, "-o", "dedup="+rstr.VolSpec.Dedup)
	}
	if len(rstr.VolSpec.Compression) != 0 {
		zfsVolArgs = append(zfsVolArgs, "-o", "compression="+rstr.VolSpec.Compression)
	}
	if len(rstr.VolSpec.Encryption) != 0 {
		zfsVolArgs = append(zfsVolArgs, "-o", "encryption="+rstr.VolSpec.Encryption)
	}
	if len(rstr.VolSpec.KeyLocation) != 0 {
		zfsVolArgs = append(zfsVolArgs, "-o", "keylocation="+rstr.VolSpec.KeyLocation)
	}
	if len(rstr.VolSpec.KeyFormat) != 0 {
		zfsVolArgs = append(zfsVolArgs, "-o", "keyformat="+rstr.VolSpec.KeyFormat)
	}

	zfsVolArgs = append(zfsVolArgs, "-F", volume)

	return zfsVolArgs
}

// builldVolumeDestroyArgs returns volume destroy command along with attributes as a string array
func buildVolumeDestroyArgs(vol *apis.ZFSVolume) []string {
	var zfsVolArgs []string

	volume := vol.Spec.PoolName + "/" + vol.Name

	zfsVolArgs = append(zfsVolArgs, ZFSDestroyArg, "-r", volume)

	return zfsVolArgs
}

func getVolume(volume string) error {
	var zfsVolArgs []string

	zfsVolArgs = append(zfsVolArgs, ZFSListArg, volume)

	cmd := exec.Command(ZFSVolCmd, zfsVolArgs...)
	_, err := cmd.CombinedOutput()
	return err
}

// CreateVolume creates the zvol/dataset as per
// info provided in ZFSVolume object
func CreateVolume(vol *apis.ZFSVolume) error {
	volume := vol.Spec.PoolName + "/" + vol.Name

	if err := getVolume(volume); err != nil {
		var args []string
		if vol.Spec.VolumeType == VolTypeDataset {
			args = buildDatasetCreateArgs(vol)
		} else {
			args = buildZvolCreateArgs(vol)
		}
		cmd := exec.Command(ZFSVolCmd, args...)
		out, err := cmd.CombinedOutput()

		if err != nil {
			klog.Errorf(
				"zfs: could not create volume %v cmd %v error: %s", volume, args, string(out),
			)
			return err
		}
		klog.Infof("created volume %s", volume)
	} else if err == nil {
		klog.Infof("using existing volume %v", volume)
	}

	return nil
}

// CreateClone creates clone for the zvol/dataset as per
// info provided in ZFSVolume object
func CreateClone(vol *apis.ZFSVolume) error {
	volume := vol.Spec.PoolName + "/" + vol.Name

	if srcVol, ok := vol.Labels[ZFSSrcVolKey]; ok {
		// datasource is volume, create the snapshot first
		snap := &apis.ZFSSnapshot{}
		snap.Name = vol.Name // use volname as snapname
		snap.Spec = vol.Spec
		// add src vol name
		snap.Labels = map[string]string{ZFSVolKey: srcVol}

		klog.Infof("creating snapshot %s@%s for the clone %s", srcVol, snap.Name, volume)

		err := CreateSnapshot(snap)

		if err != nil {
			klog.Errorf(
				"zfs: could not create snapshot for the clone vol %s snap %s err %v", volume, snap.Name, err,
			)
			return err
		}
	}

	if err := getVolume(volume); err != nil {
		args := buildCloneCreateArgs(vol)
		cmd := exec.Command(ZFSVolCmd, args...)
		out, err := cmd.CombinedOutput()

		if err != nil {
			klog.Errorf(
				"zfs: could not clone volume %v cmd %v error: %s", volume, args, string(out),
			)
			return err
		}
		klog.Infof("created clone %s", volume)
	} else if err == nil {
		klog.Infof("using existing clone volume %v", volume)
	}

	if vol.Spec.FsType == "xfs" {
		device := ZFSDevPath + volume
		return xfs.GenerateUUID(device)
	}
	if vol.Spec.FsType == "btrfs" {
		device := ZFSDevPath + volume
		return btrfs.GenerateUUID(device)
	}
	return nil
}

// SetDatasetMountProp sets mountpoint for the volume
func SetDatasetMountProp(volume string, mountpath string) error {
	var zfsVolArgs []string

	mountProperty := "mountpoint=" + mountpath
	zfsVolArgs = append(zfsVolArgs, ZFSSetArg, mountProperty, volume)

	cmd := exec.Command(ZFSVolCmd, zfsVolArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		klog.Errorf("zfs: could not set mountpoint on dataset %v cmd %v error: %s",
			volume, zfsVolArgs, string(out))
		return fmt.Errorf("could not set the mountpoint, %s", string(out))
	}
	return nil
}

// MountZFSDataset mounts the dataset to the given mountpoint
func MountZFSDataset(vol *apis.ZFSVolume, mountpath string) error {
	volume := vol.Spec.PoolName + "/" + vol.Name

	// set the mountpoint to the path where this volume should be mounted
	err := SetDatasetMountProp(volume, mountpath)
	if err != nil {
		return err
	}

	/*
	 * see if we should attempt to mount the dataset.
	 * Setting the mountpoint is sufficient to mount the zfs dataset,
	 * but if dataset has been unmounted, then setting the mountpoint
	 * is not sufficient, we have to mount the dataset explicitly
	 */
	mounted, err := GetVolumeProperty(vol, "mounted")
	if err != nil {
		return err
	}

	if mounted == "no" {
		var MountVolArg []string
		MountVolArg = append(MountVolArg, "mount", volume)
		cmd := exec.Command(ZFSVolCmd, MountVolArg...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			klog.Errorf("zfs: could not mount the dataset %v cmd %v error: %s",
				volume, MountVolArg, string(out))
			return fmt.Errorf("not able to mount, %s", string(out))
		}
	}

	return nil
}

// SetDatasetLegacyMount sets the dataset mountpoint to legacy if not set
func SetDatasetLegacyMount(vol *apis.ZFSVolume) error {
	if vol.Spec.VolumeType != VolTypeDataset {
		return nil
	}

	prop, err := GetVolumeProperty(vol, "mountpoint")
	if err != nil {
		return err
	}

	if prop != "legacy" {
		// set the mountpoint to legacy
		volume := vol.Spec.PoolName + "/" + vol.Name
		err = SetDatasetMountProp(volume, "legacy")
	}

	return err
}

// GetVolumeProperty gets zfs properties for the volume
func GetVolumeProperty(vol *apis.ZFSVolume, prop string) (string, error) {
	var zfsVolArgs []string
	volume := vol.Spec.PoolName + "/" + vol.Name

	zfsVolArgs = append(zfsVolArgs, ZFSGetArg, "-pH", "-o", "value", prop, volume)

	cmd := exec.Command(ZFSVolCmd, zfsVolArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		klog.Errorf("zfs: could not get %s on dataset %v cmd %v error: %s",
			prop, volume, zfsVolArgs, string(out))
		return "", fmt.Errorf("zfs get %s failed, %s", prop, string(out))
	}
	val := out[:len(out)-1]
	return string(val), nil
}

// SetVolumeProp sets the volume property
func SetVolumeProp(vol *apis.ZFSVolume) error {
	var err error
	volume := vol.Spec.PoolName + "/" + vol.Name

	if len(vol.Spec.Compression) == 0 &&
		len(vol.Spec.Dedup) == 0 &&
		(vol.Spec.VolumeType != VolTypeDataset ||
			len(vol.Spec.RecordSize) == 0) {
		//nothing to set, just return
		return nil
	}
	/* Case: Restart =>
	 * In this case we get the add event but here we don't know which
	 * property has changed when we were down, so firing the zfs set
	 * command with the all property present on the ZFSVolume.

	 * Case: Property Change =>
	 * TODO(pawan) When we get the update event, we make sure at least
	 * one property has changed before adding it to the event queue for
	 * handling. At this stage, since we haven't stored the
	 * ZFSVolume object as it will be too heavy, we are firing the set
	 * command with the all property preset in the ZFSVolume object since
	 * it is guaranteed that at least one property has changed.
	 */

	args := buildVolumeSetArgs(vol)
	cmd := exec.Command(ZFSVolCmd, args...)
	out, err := cmd.CombinedOutput()

	if err != nil {
		klog.Errorf(
			"zfs: could not set property on volume %v cmd %v error: %s", volume, args, string(out),
		)
		return err
	}
	klog.Infof("property set on volume %s", volume)

	return err
}

// DestroyVolume deletes the zfs volume
func DestroyVolume(vol *apis.ZFSVolume) error {
	volume := vol.Spec.PoolName + "/" + vol.Name
	parentDataset := vol.Spec.PoolName

	// check if parent dataset is present or not before attempting to delete the volume
	if err := getVolume(parentDataset); err != nil {
		klog.Errorf(
			"destroy: parent dataset %v is not present, error: %s", parentDataset, err.Error(),
		)
		return err
	}

	if err := getVolume(volume); err != nil {
		klog.Errorf(
			"destroy: volume %v is not present, error: %s", volume, err.Error(),
		)
		return nil
	}

	args := buildVolumeDestroyArgs(vol)
	cmd := exec.Command(ZFSVolCmd, args...)
	out, err := cmd.CombinedOutput()

	if err != nil {
		klog.Errorf(
			"zfs: could not destroy volume %v cmd %v error: %s", volume, args, string(out),
		)
		return err
	}

	if srcVol, ok := vol.Labels[ZFSSrcVolKey]; ok {
		// datasource is volume, delete the dependent snapshot
		snap := &apis.ZFSSnapshot{}
		snap.Name = vol.Name // snapname is same as volname
		snap.Spec = vol.Spec
		// add src vol name
		snap.Labels = map[string]string{ZFSVolKey: srcVol}

		klog.Infof("destroying snapshot %s@%s for the clone %s", srcVol, snap.Name, volume)

		err := DestroySnapshot(snap)

		if err != nil {
			// no need to reconcile as volume has already been deleted
			klog.Errorf(
				"zfs: could not destroy snapshot for the clone vol %s snap %s err %v", volume, snap.Name, err,
			)
		}
	}

	klog.Infof("destroyed volume %s", volume)

	return nil
}

// CreateSnapshot creates the zfs volume snapshot
func CreateSnapshot(snap *apis.ZFSSnapshot) error {

	volume := snap.Labels[ZFSVolKey]
	snapDataset := snap.Spec.PoolName + "/" + volume + "@" + snap.Name

	if err := getVolume(snapDataset); err == nil {
		klog.Infof("snapshot already there %s", snapDataset)
		// snapshot already there just return
		return nil
	}

	args := buildZFSSnapCreateArgs(snap)
	cmd := exec.Command(ZFSVolCmd, args...)
	out, err := cmd.CombinedOutput()

	if err != nil {
		klog.Errorf(
			"zfs: could not create snapshot %v@%v cmd %v error: %s", volume, snap.Name, args, string(out),
		)
		return err
	}
	klog.Infof("created snapshot %s@%s", volume, snap.Name)
	return nil
}

// DestroySnapshot deletes the zfs volume snapshot
func DestroySnapshot(snap *apis.ZFSSnapshot) error {

	volume := snap.Labels[ZFSVolKey]
	snapDataset := snap.Spec.PoolName + "/" + volume + "@" + snap.Name

	parentDataset := snap.Spec.PoolName

	// check if parent dataset is present or not before attempting to delete the snapshot
	if err := getVolume(parentDataset); err != nil {
		klog.Errorf(
			"destroy: snapshot's(%v) parent dataset %v is not present, error: %s",
			snapDataset, parentDataset, err.Error(),
		)
		return err
	}

	if err := getVolume(snapDataset); err != nil {
		klog.Errorf(
			"destroy: snapshot %v is not present, error: %s", volume, err.Error(),
		)
		return nil
	}

	args := buildZFSSnapDestroyArgs(snap)
	cmd := exec.Command(ZFSVolCmd, args...)
	out, err := cmd.CombinedOutput()

	if err != nil {
		klog.Errorf(
			"zfs: could not destroy snapshot %v@%v cmd %v error: %s", volume, snap.Name, args, string(out),
		)
		return err
	}
	klog.Infof("deleted snapshot %s@%s", volume, snap.Name)
	return nil
}

// GetVolumeDevPath returns devpath for the given volume
func GetVolumeDevPath(vol *apis.ZFSVolume) (string, error) {
	volume := vol.Spec.PoolName + "/" + vol.Name
	if vol.Spec.VolumeType == VolTypeDataset {
		return volume, nil
	}

	devicePath := ZFSDevPath + volume

	// evaluate the symlink to get the dev path for zvol
	dev, err := filepath.EvalSymlinks(devicePath)
	if err != nil {
		return "", err
	}

	return dev, nil
}

// ResizeZFSVolume resize volume
func ResizeZFSVolume(vol *apis.ZFSVolume, mountpath string, resizefs bool) error {

	volume := vol.Spec.PoolName + "/" + vol.Name
	args := buildVolumeResizeArgs(vol)
	cmd := exec.Command(ZFSVolCmd, args...)
	out, err := cmd.CombinedOutput()

	if err != nil {
		klog.Errorf(
			"zfs: could not resize the volume %v cmd %v error: %s", volume, args, string(out),
		)
		return err
	}

	if resizefs {
		// resize the filesystem so that applications can use the expanded space
		err = handleVolResize(vol, mountpath)
	}

	return err
}

// CreateBackup creates the backup
func CreateBackup(bkp *apis.ZFSBackup) error {
	vol, err := GetZFSVolume(bkp.Spec.VolumeName)
	if err != nil {
		return err
	}

	volume := vol.Spec.PoolName + "/" + vol.Name

	/* create the snapshot for the backup */
	snap := &apis.ZFSSnapshot{}
	snap.Name = bkp.Spec.SnapName
	snap.Spec.PoolName = vol.Spec.PoolName
	snap.Labels = map[string]string{ZFSVolKey: vol.Name}

	err = CreateSnapshot(snap)

	if err != nil {
		klog.Errorf(
			"zfs: could not create snapshot for the backup vol %s snap %s err %v", volume, snap.Name, err,
		)
		return err
	}

	rawConn, err := net.Dial("tcp", bkp.Spec.BackupDest)
	if err != nil {
		return err
	}

	conn := NewConnWithErr(rawConn)

	defer conn.Close()

	args := buildVolumeBackupArgs(bkp, vol)
	cmd := exec.Command(ZFSVolCmd, args...)

	cmd.Stdout = conn

	out := bytes.NewBuffer(nil)
	cmd.Stderr = out

	err = cmd.Run()
	if err != nil {
		klog.Errorf(
			"zfs: could not backup the volume %v cmd %v error: %s; network error: %v",
			volume, args, out.String(), conn.Err(),
		)

		return err
	}

	if err := conn.Err(); err != nil {
		klog.Errorf(
			"zfs: could not backup the volume due to network error %v cmd %v error: %s; network error: %v",
			volume, args, out.String(), conn.Err(),
		)

		return err
	}

	return nil
}

// DestoryBackup deletes the snapshot created
func DestoryBackup(bkp *apis.ZFSBackup) error {
	vol, err := GetZFSVolume(bkp.Spec.VolumeName)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			// Volume has been deleted, return
			return nil
		}
		return err
	}

	volume := vol.Spec.PoolName + "/" + vol.Name

	/* create the snapshot for the backup */
	snap := &apis.ZFSSnapshot{}
	snap.Name = bkp.Spec.SnapName
	snap.Spec.PoolName = vol.Spec.PoolName
	snap.Labels = map[string]string{ZFSVolKey: vol.Name}

	err = DestroySnapshot(snap)

	if err != nil {
		klog.Errorf(
			"zfs: could not destroy snapshot for the backup vol %s snap %s err %v", volume, snap.Name, err,
		)
	}
	return err
}

// getDevice waits for the device to be created and returns the devpath
func getDevice(volume string) (string, error) {
	device := ZFSDevPath + volume
	// device should be created within 5 seconds
	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-timeout:
			return "", fmt.Errorf("zfs: not able to get the device: %s", device)
		default:
			if _, err := os.Stat(device); err == nil {
				return device, nil
			}
		}
		time.Sleep(1 * time.Second)
	}
}

// CreateRestore creates the restore
func CreateRestore(rstr *apis.ZFSRestore) error {
	if len(rstr.VolSpec.PoolName) == 0 {
		// for backward compatibility, older version of
		// velero will not add spec in the ZFSRestore Object
		// query it here and fill that information
		vol, err := GetZFSVolume(rstr.Spec.VolumeName)
		if err != nil {
			return err
		}
		rstr.VolSpec = vol.Spec
	}
	args := buildVolumeRestoreArgs(rstr)

	volume := rstr.VolSpec.PoolName + "/" + rstr.Spec.VolumeName

	conn, err := net.Dial("tcp", rstr.Spec.RestoreSrc)
	if err != nil {
		return err
	}

	defer conn.Close()

	cmd := exec.Command(ZFSVolCmd, args...)
	cmd.Stdin = conn
	out, err := cmd.CombinedOutput()
	if err != nil {
		klog.Errorf(
			"zfs: could not restore the volume %v cmd %v error: %s", volume, args, string(out),
		)
		return err
	}

	/*
	 * need to generate a new uuid for zfs and btrfs volumes
	 * so that we can mount it.
	 */
	if rstr.VolSpec.FsType == "xfs" {
		device, err := getDevice(volume)
		if err != nil {
			return err
		}
		return xfs.GenerateUUID(device)
	}
	if rstr.VolSpec.FsType == "btrfs" {
		device, err := getDevice(volume)
		if err != nil {
			return err
		}
		return btrfs.GenerateUUID(device)
	}

	return nil
}

// ListZFSPool invokes `zfs list` to list all the available
// pools in the node.
func ListZFSPool() ([]apis.Pool, error) {
	args := []string{
		ZFSListArg, "-s", "name",
		"-o", "name,guid,available",
		"-H", "-p",
	}
	cmd := exec.Command(ZFSVolCmd, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		klog.Errorf("zfs: could not list zpool cmd %v: %v", args, err)
		return nil, err
	}
	return decodeListOutput(output)
}

// The `zfs list` command will list down all the resources including
// pools and volumes and as the pool names cannot have "/" in the name
// the function below filters out the pools. Sample output of command:
// $ zfs list -s name -o name,guid,available -H -p
// zfspv-pool	4734063099997348493	103498467328
// zfspv-pool/pvc-be02d230-3738-4de9-8968-70f5d10d86dd	3380225606535803752	4294942720
func decodeListOutput(raw []byte) ([]apis.Pool, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	pools := []apis.Pool{}
	for scanner.Scan() {
		items := strings.Split(strings.TrimSpace(scanner.Text()), "\t")
		if !strings.Contains(items[0], "/") {
			var pool apis.Pool
			pool.Name = items[0]
			pool.UUID = items[1]
			sizeBytes, err := strconv.ParseInt(items[2],
				10, 64)
			if err != nil {
				err = fmt.Errorf("cannot get free size for pool %v: %v", pool.Name, err)
				return pools, err
			}
			pool.Free = *resource.NewQuantity(sizeBytes, resource.BinarySI)
			pools = append(pools, pool)
		}
	}
	return pools, nil
}
