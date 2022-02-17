package types

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/pkg/errors"

	"github.com/projecteru2/core/resources"
	"github.com/projecteru2/core/utils"
)

const auto = "AUTO"

// VolumeMap is volume map
// VolumeMap {["/data1"]1073741824, ["/data2"]1048576}
type VolumeMap resources.ResourceMap

// DeepCopy .
func (v VolumeMap) DeepCopy() VolumeMap {
	res := VolumeMap{}
	for key, value := range v {
		res[key] = value
	}
	return res
}

// Total .
func (v VolumeMap) Total() int64 {
	res := int64(0)
	for _, value := range v {
		res += value
	}
	return res
}

// Add .
func (v VolumeMap) Add(q VolumeMap) {
	for label, value := range q {
		if _, ok := v[label]; !ok {
			v[label] = value
		} else {
			v[label] += value
		}
	}
}

// Sub .
func (v VolumeMap) Sub(q VolumeMap) {
	for label, value := range q {
		if _, ok := v[label]; ok {
			v[label] -= value
		}
	}
}

// GetResourceID returns device name such as "/sda0"
// GetResourceID only works for VolumeMap with single key
func (v VolumeMap) GetResourceID() (key string) {
	for k := range v {
		key = k
		break
	}
	return
}

// GetRation returns scheduled size from device
// GetRation only works for VolumeMap with single key
func (v VolumeMap) GetRation() int64 {
	return v[v.GetResourceID()]
}

// SplitByUsed .
func (v VolumeMap) SplitByUsed(init VolumeMap) (VolumeMap, VolumeMap) {
	used := VolumeMap{}
	unused := VolumeMap{}
	for mountDir, freeSpace := range v {
		vmap := used
		if init[mountDir] == freeSpace {
			vmap = unused
		}
		vmap.Add(VolumeMap{mountDir: freeSpace})
	}
	return used, unused
}

// VolumeBinding src:dst:flags:size
type VolumeBinding struct {
	Source      string
	Destination string
	Flags       string
	SizeInBytes int64
}

// NewVolumeBinding returns pointer of VolumeBinding
func NewVolumeBinding(volume string) (_ *VolumeBinding, err error) {
	var src, dst, flags string
	var size int64

	parts := strings.Split(volume, ":")
	switch len(parts) {
	case 2:
		src, dst = parts[0], parts[1]
	case 3:
		src, dst, flags = parts[0], parts[1], parts[2]
	case 4:
		src, dst, flags = parts[0], parts[1], parts[2]
		if size, err = utils.ParseRAMInHuman(parts[3]); err != nil {
			return nil, errors.WithStack(err)
		}
	default:
		return nil, errors.WithStack(fmt.Errorf("invalid volume: %v", volume))
	}

	flagParts := strings.Split(flags, "")
	sort.Strings(flagParts)

	vb := &VolumeBinding{
		Source:      src,
		Destination: dst,
		Flags:       strings.Join(flagParts, ""),
		SizeInBytes: size,
	}
	return vb, vb.Validate()
}

// Validate return error if invalid
func (vb VolumeBinding) Validate() error {
	if vb.Destination == "" {
		return errors.WithStack(errors.Errorf("invalid volume, dest must be provided: %v", vb))
	}
	if vb.RequireScheduleMonopoly() && vb.RequireScheduleUnlimitedQuota() {
		return errors.WithStack(errors.Errorf("invalid volume, monopoly volume must not be limited: %v", vb))
	}
	return nil
}

// RequireSchedule returns true if volume binding requires schedule
func (vb VolumeBinding) RequireSchedule() bool {
	return strings.HasSuffix(vb.Source, auto)
}

// RequireScheduleUnlimitedQuota .
func (vb VolumeBinding) RequireScheduleUnlimitedQuota() bool {
	return vb.RequireSchedule() && vb.SizeInBytes == 0
}

// RequireScheduleMonopoly returns true if volume binding requires monopoly schedule
func (vb VolumeBinding) RequireScheduleMonopoly() bool {
	return vb.RequireSchedule() && strings.Contains(vb.Flags, "m")
}

// ToString returns volume string
func (vb VolumeBinding) ToString(normalize bool) (volume string) {
	flags := vb.Flags
	if normalize {
		flags = strings.ReplaceAll(flags, "m", "")
	}

	if strings.Contains(flags, "o") {
		flags = strings.ReplaceAll(flags, "o", "")
		flags = strings.ReplaceAll(flags, "r", "ro")
		flags = strings.ReplaceAll(flags, "w", "wo")
	}

	switch {
	case vb.Flags == "" && vb.SizeInBytes == 0:
		volume = fmt.Sprintf("%s:%s", vb.Source, vb.Destination)
	default:
		volume = fmt.Sprintf("%s:%s:%s:%d", vb.Source, vb.Destination, flags, vb.SizeInBytes)
	}
	return volume
}

// GetMapKey .
func (vb VolumeBinding) GetMapKey() [3]string {
	return [3]string{vb.Source, vb.Destination, vb.Flags}
}

// VolumeBindings is a collection of VolumeBinding
type VolumeBindings []*VolumeBinding

// NewVolumeBindings return VolumeBindings of reference type
func NewVolumeBindings(volumes []string) (volumeBindings VolumeBindings, err error) {
	for _, vb := range volumes {
		volumeBinding, err := NewVolumeBinding(vb)
		if err != nil {
			return nil, err
		}
		volumeBindings = append(volumeBindings, volumeBinding)
	}
	return
}

// ToStringSlice converts VolumeBindings into string slice
func (vbs VolumeBindings) ToStringSlice(sorted, normalize bool) (volumes []string) {
	if sorted {
		sort.Slice(vbs, func(i, j int) bool { return vbs[i].ToString(false) < vbs[j].ToString(false) })
	}
	for _, vb := range vbs {
		volumes = append(volumes, vb.ToString(normalize))
	}
	return
}

// UnmarshalJSON is used for encoding/json.Unmarshal
func (vbs *VolumeBindings) UnmarshalJSON(b []byte) (err error) {
	volumes := []string{}
	if err = json.Unmarshal(b, &volumes); err != nil {
		return errors.WithStack(err)
	}
	*vbs, err = NewVolumeBindings(volumes)
	return
}

// MarshalJSON is used for encoding/json.Marshal
func (vbs VolumeBindings) MarshalJSON() ([]byte, error) {
	volumes := []string{}
	for _, vb := range vbs {
		volumes = append(volumes, vb.ToString(false))
	}
	bs, err := json.Marshal(volumes)
	return bs, errors.WithStack(err)
}

// ApplyPlan creates new VolumeBindings according to volume plan
func (vbs VolumeBindings) ApplyPlan(plan VolumePlan) (res VolumeBindings) {
	for _, vb := range vbs {
		newVb := &VolumeBinding{vb.Source, vb.Destination, vb.Flags, vb.SizeInBytes}
		if vmap, _ := plan.GetVolumeMap(vb); vmap != nil {
			newVb.Source = vmap.GetResourceID()
		}
		res = append(res, newVb)
	}
	return
}

// Divide .
func (vbs VolumeBindings) Divide() (soft VolumeBindings, hard VolumeBindings) {
	for _, vb := range vbs {
		if strings.HasSuffix(vb.Source, auto) {
			soft = append(soft, vb)
		} else {
			hard = append(hard, vb)
		}
	}
	return
}

// IsEqual return true is two VolumeBindings have the same value
func (vbs VolumeBindings) IsEqual(vbs2 VolumeBindings) bool {
	return reflect.DeepEqual(vbs.ToStringSlice(true, false), vbs2.ToStringSlice(true, false))
}

// TotalSize .
func (vbs VolumeBindings) TotalSize() (total int64) {
	for _, vb := range vbs {
		total += vb.SizeInBytes
	}
	return
}

// MergeVolumeBindings combines two VolumeBindings
func MergeVolumeBindings(vbs1 VolumeBindings, vbs2 ...VolumeBindings) (vbs VolumeBindings) {
	sizeMap := map[[3]string]int64{} // {["AUTO", "/data", "rw"]: 100}
	for _, vbs := range append(vbs2, vbs1) {
		for _, vb := range vbs {
			key := [3]string{vb.Source, vb.Destination, vb.Flags}
			sizeMap[key] += vb.SizeInBytes
		}
	}

	for key, size := range sizeMap {
		if size < 0 {
			continue
		}
		vbs = append(vbs, &VolumeBinding{
			Source:      key[0],
			Destination: key[1],
			Flags:       key[2],
			SizeInBytes: size,
		})
	}
	return
}

// VolumePlan is map from volume string to volumeMap: {"AUTO:/data:rw:100": VolumeMap{"/sda1": 100}}
type VolumePlan map[VolumeBinding]VolumeMap

// MakeVolumePlan creates VolumePlan pointer by volume strings and scheduled VolumeMaps
func MakeVolumePlan(vbs VolumeBindings, distribution []VolumeMap) VolumePlan {
	sort.Slice(vbs, func(i, j int) bool { return vbs[i].SizeInBytes < vbs[j].SizeInBytes })
	sort.Slice(distribution, func(i, j int) bool { return distribution[i].GetRation() < distribution[j].GetRation() })

	volumePlan := VolumePlan{}
	for idx, vb := range vbs {
		if vb.RequireSchedule() {
			volumePlan[*vb] = distribution[idx]
		}
	}
	return volumePlan
}

// UnmarshalJSON .
func (p *VolumePlan) UnmarshalJSON(b []byte) (err error) {
	if *p == nil {
		*p = VolumePlan{}
	}
	plan := map[string]VolumeMap{}
	if err = json.Unmarshal(b, &plan); err != nil {
		return errors.WithStack(err)
	}
	for volume, vmap := range plan {
		vb, err := NewVolumeBinding(volume)
		if err != nil {
			return errors.WithStack(err)
		}
		(*p)[*vb] = vmap
	}
	return
}

// MarshalJSON .
func (p VolumePlan) MarshalJSON() ([]byte, error) {
	plan := map[string]VolumeMap{}
	for vb, vmap := range p {
		plan[vb.ToString(false)] = vmap
	}
	bs, err := json.Marshal(plan)
	return bs, errors.WithStack(err)
}

// ToLiteral returns literal VolumePlan
func (p VolumePlan) ToLiteral() map[string]map[string]int64 {
	plan := map[string]map[string]int64{}
	for vb, volumeMap := range p {
		plan[vb.ToString(false)] = volumeMap
	}
	return plan
}

// IntoVolumeMap Merge return one VolumeMap with all in VolumePlan added
func (p VolumePlan) IntoVolumeMap() VolumeMap {
	volumeMap := VolumeMap{}
	for _, v := range p {
		volumeMap.Add(v)
	}
	return volumeMap
}

// GetVolumeMap looks up VolumeMap according to volume destination directory
func (p VolumePlan) GetVolumeMap(vb *VolumeBinding) (volMap VolumeMap, volume VolumeBinding) {
	for volume, volMap := range p {
		if vb.Destination == volume.Destination {
			return volMap, volume
		}
	}
	return
}

// Compatible return true if new bindings stick to the old bindings
func (p VolumePlan) Compatible(oldPlan VolumePlan) bool {
	for volume, oldBinding := range oldPlan {
		newBinding, _ := p.GetVolumeMap(&volume)
		// newBinding is ok to be nil when reallocing requires less volumes than before
		if newBinding != nil && newBinding.GetResourceID() != oldBinding.GetResourceID() {
			// unlimited binding, modify binding source
			if newBinding.GetRation() == 0 {
				// p[v] = VolumeMap{oldBinding.GetResourceID(): 0}
				continue
			}
			return false
		}
	}
	return true
}

// Merge .
func (p VolumePlan) Merge(p2 VolumePlan) {
	for vb, vm := range p2 {
		p[vb] = vm
	}
}

// FindAffinityPlan .
func (p VolumePlan) FindAffinityPlan(req VolumeBinding) (_ VolumeBinding, _ VolumeMap, found bool) {
	for vb, vm := range p {
		if vb.Source == req.Source && vb.Destination == req.Destination && vb.Flags == req.Flags {
			return vb, vm, true
		}
	}
	found = false
	return
}
