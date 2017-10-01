package qinsert

import (
	"regexp"
)

var (
	reinsert  *regexp.Regexp
	retable   *regexp.Regexp
	recolumns *regexp.Regexp
	revalues  *regexp.Regexp
	recomma   *regexp.Regexp


	remath            *regexp.Regexp
	reconststr        *regexp.Regexp
	reconstint        *regexp.Regexp
	refunc            *regexp.Regexp

)

func init() {
	reinsert = regexp.MustCompile(`^(?i)(\s*insert\s+into\s+)`)
	retable = regexp.MustCompile(`^(?i)(\s*\w+(\.\w+){0,1}\s*)`)
	recolumns = regexp.MustCompile(`^(?i)\((\s*\w+(?:\s*,\s*\w+){1,100})\s*\)\s*`)
	revalues = regexp.MustCompile(`^(?i)(\s*values\s*)`)
	recomma = regexp.MustCompile(`^\s*,\s*`)

	remath = regexp.MustCompile(`^(?i)(\s*\d+[\+\-\*\/]\d+\s*)`)
	reconststr = regexp.MustCompile(`^(?i)(\s*\@strval\d+\s*)`)
	reconstint = regexp.MustCompile(`^(?i)(\s*\d+\s*)`)
	refunc = regexp.MustCompile(`^(?i)(\s*\w+\(.*?\)\s*)`)
}

func matchAndReplace(r *regexp.Regexp, s string) (match string, rs string, rb bool) {
	if r.MatchString(s) {
		return r.FindString(s), string(r.ReplaceAll([]byte(s), []byte{})), true
	}
	return "", s, false

}

func parseByRegexp(re *regexp.Regexp, q string) (match interface{}, rs string, rb bool) {
	return matchAndReplace(re, q)
}
