package mydump2bq

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

var valuesExp *regexp.Regexp

func init() {
	valuesExp = regexp.MustCompile("^INSERT INTO `(.+?)` VALUES \\((.+)\\);$")
}

func main() {
	buf := []byte{}
	sc := bufio.NewScanner(os.Stdin)
}

type Streamer struct {
}

type Scanner struct {
	Buf        []byte
	MaxBufSize int
	*bufio.Scanner
}

func NewScanner(r io.Reader, maxBufSize int) *Scanner {
	sc := &Scanner{
		Buf:        []byte{},
		MaxBufSize: maxBufSize,
		Scanner:    bufio.NewScanner(r),
	}
	sc.Scanner.Buffer(sc.Buf, sc.MaxBufSize)
	return sc
}

func (sc *Scanner) Scan() {
	for sc.Scanner.Scan() {
		t := sc.Scanner.Text()
		if !strings.HasPrefix(t, "INSERT INTO") {
			continue
		}

		matchStr := valuesExp.FindAllString(t, 2)
		if matchStr == nil {
			continue
		}
		table := matchStr[0]
		values := matchStr[1]
	}
}

func (sc *Scanner) parse(str string) ([]string, error) {
	values := make([]string, 0, 8)

	i := 0
	for i < len(str) {
		if str[i] != '\'' {
			// no string, read until comma
			j := i + 1
			for ; j < len(str) && str[j] != ','; j++ {
			}
			values = append(values, str[i:j])
			// skip ,
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

			value := str[i : j+1]
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
