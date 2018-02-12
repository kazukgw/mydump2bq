package mydump2bq

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

var valuesExp *regexp.Regexp

func init() {
	valuesExp = regexp.MustCompile("^INSERT INTO `(.+?)` VALUES \\((.+)\\);$")
}

type Scanner struct {
	ID         string
	MaxBufSize int
	Scanner    *bufio.Scanner
	*TableMap
}

func NewScanner(
	r io.Reader,
	maxBufSize int,
	tmap *TableMap,
) (*Scanner, error) {
	buf := []byte{}
	sc := bufio.NewScanner(r)
	sc.Buffer(buf, maxBufSize)
	id, err := uuid.NewV4()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	scanner := &Scanner{
		ID:         id.String(),
		MaxBufSize: maxBufSize,
		Scanner:    sc,
		TableMap:   tmap,
	}
	return scanner, nil
}

func (sc *Scanner) Scan() (*Row, error) {
	var row *Row
	for {
		ok := sc.Scanner.Scan()
		if err := sc.Scanner.Err(); err != nil {
			log.Debug("scanner error")
			return nil, err
		}
		if !ok {
			log.Debug("scanner encountered EOF")
			return nil, io.EOF
		}
		log.Debug("scanner call text() method")
		line := sc.Scanner.Text()
		if !strings.HasPrefix(line, "INSERT INTO") {
			continue
		}
		log.Debug("raw values match with regexp")
		if m := valuesExp.FindAllStringSubmatch(line, -1); len(m) == 1 {
			values, err := sc.parseValues(m[0][2])
			if err != nil {
				return nil, errors.Errorf("parse values %v err", line)
			}
			row = NewRow(sc.TableMap, values, sc.ID)
			break
		}
	}
	return row, nil
}

func (sc *Scanner) parseValues(str string) ([]string, error) {
	// values are seperated by comma, but we can not split using comma directly
	// string is enclosed by single quote

	// a simple implementation, may be more robust later.

	values := make([]string, 0, 8)

	i := 0
	for i < len(str) {
		if str[i] != '\'' {
			// no string, read until comma
			j := i + 1
			for ; j < len(str) && str[j] != ','; j++ {
			}

			values = append(values, str[i:j])

			i = j + 1
		} else {
			// read string until another single quote
			j := i + 1

			escaped := false
			for j < len(str) {
				if str[j] == '\\' {
					// skip escaped character
					j += 2
					escaped = true
					continue
				} else if str[j] == '\'' {
					break
				} else {
					j++
				}
			}

			if j >= len(str) {
				return nil, fmt.Errorf("parse quote values error")
			}

			value := str[i+1 : j]
			if escaped {
				value = sc.unescapeString(value)
			}
			values = append(values, value)
			// skip ' and ,
			i = j + 2
		}

		// need skip blank???
	}

	return values, nil
}

// unescapeString un-escapes the string.
// mysqldump will escape the string when dumps,
// Refer http://dev.mysql.com/doc/refman/5.7/en/string-literals.html
func (sc *Scanner) unescapeString(s string) string {
	i := 0

	value := make([]byte, 0, len(s))
	for i < len(s) {
		if s[i] == '\\' {
			j := i + 1
			if j == len(s) {
				// The last char is \, remove
				break
			}

			value = append(value, sc.unescapeChar(s[j]))
			i += 2
		} else {
			value = append(value, s[i])
			i++
		}
	}

	return string(value)
}

func (sc *Scanner) unescapeChar(ch byte) byte {
	// \" \' \\ \n \0 \b \Z \r \t ==> escape to one char
	switch ch {
	case 'n':
		ch = '\n'
	case '0':
		ch = 0
	case 'b':
		ch = 8
	case 'Z':
		ch = 26
	case 'r':
		ch = '\r'
	case 't':
		ch = '\t'
	}
	return ch
}
