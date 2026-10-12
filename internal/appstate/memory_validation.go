package appstate

import (
	"k8s.io/apimachinery/pkg/util/validation"
	"slices"
)

func validStreamLocation(namespace, name, uid string, c Collector) bool {
	return dns(namespace) && slices.Contains(c.Namespaces, namespace) && len(validation.IsDNS1123Subdomain(name)) == 0 && len(uid) > 0 && len(uid) <= 128
}

func validStreamResource(o ResourceObservation, c Collector) bool {
	if !validStreamLocation(o.Namespace, o.Name, o.UID, c) || len(o.ResourceVersion) == 0 || len(o.ResourceVersion) > 256 ||
		o.Generation < 0 || o.ObservedGeneration < 0 || o.Desired < 0 || o.Replicas < 0 || o.Ready < 0 || o.Updated < 0 || o.Available < 0 ||
		!slices.Contains([]string{"not_checked", "ready", "failed"}, o.Health) {
		return false
	}
	for _, id := range []string{o.StationID, o.OrganizationID, o.InstallationID, o.AppID, o.InstanceID} {
		if len(id) > 128 {
			return false
		}
	}
	if m := o.Metadata; m != nil {
		if len(m.TemplateHash) > 128 || m.Images == nil || len(m.Images) > 64 || (!m.ImagesComplete && len(m.Images) != 0) || (m.ImagesComplete && len(m.Images) == 0) {
			return false
		}
		seen := map[string]bool{}
		for _, image := range m.Images {
			if !dns(image.Component) || len(image.Image) == 0 || len(image.Image) > 2048 || seen[image.Component] {
				return false
			}
			seen[image.Component] = true
		}
		d := m.Discovery
		if len(d.AppID) > 128 || len(d.GroupID) > 128 || len(d.Brand) > 128 || len(d.Version) > 128 || len(d.DisplayName) > 512 || len(d.Models) > 4096 || len(d.Source) > 1024 {
			return false
		}
	}
	return true
}
