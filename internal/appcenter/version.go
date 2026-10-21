package appcenter

import (
	"strconv"
	"strings"
)

// VersionNewer reports whether target is a strictly higher SemVer than
// installed. Unparsable versions never qualify, so no upgrade is offered
// rather than a possible downgrade.
func VersionNewer(target, installed string) bool {
	t, tPre, ok := parseSemVer(target)
	if !ok {
		return false
	}
	i, iPre, ok := parseSemVer(installed)
	if !ok {
		return false
	}
	for k := range t {
		if t[k] != i[k] {
			return t[k] > i[k]
		}
	}
	// Same core version: a release outranks any pre-release of it.
	switch {
	case tPre == iPre:
		return false
	case tPre == "":
		return true
	case iPre == "":
		return false
	}
	return comparePreRelease(tPre, iPre) > 0
}

func parseSemVer(value string) ([3]int, string, bool) {
	var core [3]int
	value = strings.TrimPrefix(strings.TrimSpace(value), "v")
	value, _, _ = strings.Cut(value, "+")
	base, pre, _ := strings.Cut(value, "-")
	parts := strings.Split(base, ".")
	if len(parts) != 3 {
		return core, "", false
	}
	for k, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 || part == "" || (len(part) > 1 && part[0] == '0') {
			return core, "", false
		}
		core[k] = n
	}
	return core, pre, true
}

func comparePreRelease(a, b string) int {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for k := 0; k < len(as) && k < len(bs); k++ {
		an, aErr := strconv.Atoi(as[k])
		bn, bErr := strconv.Atoi(bs[k])
		switch {
		case aErr == nil && bErr == nil && an != bn:
			if an > bn {
				return 1
			}
			return -1
		case aErr == nil && bErr != nil:
			return -1
		case aErr != nil && bErr == nil:
			return 1
		case aErr != nil && as[k] != bs[k]:
			return strings.Compare(as[k], bs[k])
		}
	}
	return len(as) - len(bs)
}
