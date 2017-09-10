package sql

import (
	"fmt"
	"regexp"
	"ddb/sql/qselect"
)

type Parser struct {
	query   string
	strvars map[string]string
}

func (p *Parser) Parse(query string) (*qselect.Select) {
	p.strvars = map[string]string{}
	p.query = query
	p.cutStrings()
	p.normilizeWhiteSpaces()

	result := qselect.CreateSelectFromString(p.query)

	if result != nil {
		for i := range result.Where {
			val, ok := p.GetConstValue(result.Where[i].OperandB)
			if ok {
				result.Where[i].OperandB = val
			}
		}
	}

	return result
}

func (p *Parser) GetQuery() string {
	return p.query
}

func (p *Parser) cutStrings() {
	quotes := []string{`"`, `'`}
	for _, quote := range quotes {
		for {
			if r, from, to := p.matchStringValue(p.query, quote); !r {
				break
			} else {
				key := fmt.Sprintf("@strval%d", 10000+len(p.strvars))

				p.strvars[key] = p.query[from+1:to]

				p.query = p.query[:from] + key + p.query[to+1:]
			}
		}
	}
}

func (p *Parser) matchQuote(s string, quote, slash string, from int) (bool, int) {
	for i := from; i < len(s); i++ {
		if s[i] == quote[0] && (i == 0 || s[i-1] != slash[0]) {
			return true, i
		}
	}
	return false, -1
}

func (p *Parser) matchStringValue(s string, quote string) (r bool, from, to int) {
	if r, from = p.matchQuote(s, quote, `\`, 0); !r {
		return false, -1, -1
	}

	if r, to = p.matchQuote(s, quote, `\`, from+1); !r {
		return false, -1, -1
	}

	return r, from, to
}

func (p *Parser) normilizeWhiteSpaces() {
	r := regexp.MustCompile(`\s+`)
	p.query = string(r.ReplaceAll([]byte(p.query), []byte(" ")))
}

func (p *Parser) GetConstValue(key string) (string, bool) {
	if val, ok := p.strvars[key]; ok {
		return val, ok
	}
	return "", false
}