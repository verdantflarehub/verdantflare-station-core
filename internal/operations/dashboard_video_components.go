package operations

type dashboardManifestImage struct {
	Component string `json:"component"`
	Image     string `json:"image"`
}

// The first managed Video transition retains its historical authorization
// sidecar; other applications retain the single-image admission rule.
func dashboardImagesMatch(images []dashboardManifestImage, release transitionReleaseIndex) bool {
	if release.AppID != "video-mcp" {
		return len(images) == 1 && len(release.Components) == 0 && images[0].Image == release.AppImage
	}
	if (release.AppVersion != "0.17.28" && release.AppVersion != "0.17.29" && release.AppVersion != "0.17.30" && release.AppVersion != "0.17.31") || len(images) != 2 || len(release.Components) != 2 || images[0].Component != "api" || images[1].Component != "artifact-authority" {
		return false
	}
	for i, image := range images {
		component := release.Components[i]
		if image.Component != component.Name || image.Image != component.Image || !transitionDigest.MatchString(component.ImageDigest) {
			return false
		}
	}
	return release.Components[0].Image == release.AppImage && release.Components[0].ImageDigest == release.AppImageDigest &&
		release.Components[1].Image == "registry.cn-qingdao.aliyuncs.com/wod/verdantflare-app:video-mcp-server-v0.17.12" &&
		release.Components[1].ImageDigest == "sha256:b4a6eed372cca5ede2a3722a45de210b3bac223050e3e478e89943f997b0a4a4"
}
