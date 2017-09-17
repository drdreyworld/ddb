package qselect

import (
	"regexp"
)

var (
	reselect          *regexp.Regexp
	recomma           *regexp.Regexp
	remath            *regexp.Regexp
	reconststr        *regexp.Regexp
	reconstint        *regexp.Regexp
	refunc            *regexp.Regexp
	recolumn          *regexp.Regexp
	recolumnWithAlias *regexp.Regexp
	refrom            *regexp.Regexp
	refromtable       *regexp.Regexp
	rewhere           *regexp.Regexp
	reandor           *regexp.Regexp
	reenum            *regexp.Regexp
	reorder           *regexp.Regexp
	reorderdir        *regexp.Regexp
	relimit           *regexp.Regexp
)

func init() {
	reselect = regexp.MustCompile(`^(?i)(\s*select\s*)`)
	recomma = regexp.MustCompile(`^\s*,\s*`)
	remath = regexp.MustCompile(`^(?i)(\s*\d+[\+\-\*\/]\d+\s*)`)
	reconststr = regexp.MustCompile(`^(?i)(\s*\@strval\d+\s*)`)
	reconstint = regexp.MustCompile(`^(?i)(\s*\d+\s*)`)
	refunc = regexp.MustCompile(`^(?i)(\s*\w+\(.*?\)\s*)`)
	recolumnWithAlias = regexp.MustCompile(`^(?i)(\s*\w+(\.\w+){0,1}(\s+as\s+\w+){0,1}\s*)`)
	recolumn = regexp.MustCompile(`^(?i)(\s*\w+(\.\w+){0,1}\s*)`)
	refrom = regexp.MustCompile(`^(?i)(\s*from\s*)`)
	refromtable = regexp.MustCompile(`^(?i)(\s*\w+(\.\w+){0,1}(\s+as\s+\w+){0,1}\s*)`)
	rewhere = regexp.MustCompile(`^(?i)(\s*where\s*)`)
	reandor = regexp.MustCompile(`^(?i)(\s*(and|or)\s+)`)
	reenum = regexp.MustCompile(`^(?i)(\s*\((.*?)(,.*?){0,}\)\s*)`)
	reorder = regexp.MustCompile(`^(?i)(\s*order\s+by\s+)`)
	reorderdir = regexp.MustCompile(`^(?i)(\s*(ASC|DESC))`)
	relimit = regexp.MustCompile(`^(?i)(\s*limit\s+(\d+)(?:,\s*(\d+)){0,1}\s*)`)
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