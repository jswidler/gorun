package columns

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jswidler/gorun/logger"
)

type column struct {
	name string
	val  any
}

type Columns struct {
	cs []column
}

func Of(ob any) Columns {
	v := reflect.ValueOf(ob)
	for k := v.Kind(); k == reflect.Ptr || k == reflect.Interface; {
		v = v.Elem()
		k = v.Kind()
	}

	if v.Kind() != reflect.Struct {
		logger.Default().Fatal().Msgf("unable to get db columns from type %s", v.Kind())
	}

	l := []column{}
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		dbCol := field.Tag.Get("db")
		if dbCol != "" {
			l = append(l, column{dbCol, v.Field(i).Interface()})
		}
	}
	return Columns{l}
}

func (c Columns) Names() []string {
	cols := make([]string, 0, len(c.cs))
	for i := range c.cs {
		cols = append(cols, c.cs[i].name)
	}
	return cols
}

// Columns returns a comman separated list of column names that are wrappeed in quotes, e.g. "col1", "col2", "col3".
// Suitable for use in SQL queries - but does not check the column names are safe.
func (c Columns) Columns() string {
	if len(c.cs) == 0 {
		return ""
	}
	cols := make([]string, 0, len(c.cs))
	for i := range c.cs {
		cols = append(cols, c.cs[i].name)
	}
	return `"` + strings.Join(cols, `", "`) + `"`
}

// ColumnsPlaceholder returns a string of comma separated placeholders for the given number of parameters, starting at "from".
// For example, paramString(6, 2) returns "$6, $7".
func (c Columns) ColumnsPlaceholder(from int) string {
	s := make([]string, len(c.cs))
	for i := 0; i < len(c.cs); i++ {
		s[i] = fmt.Sprintf("$%d", i+from)
	}
	return strings.Join(s, ", ")
}

func (c Columns) ColumnsNamedPlaceholder() string {
	s := make([]string, len(c.cs))
	for i := range c.cs {
		s[i] = fmt.Sprintf(":%s", c.cs[i].name)
	}
	return strings.Join(s, ", ")
}

func (c Columns) Get(columnName string) (any, bool) {
	for _, col := range c.cs {
		if col.name == columnName {
			return col.val, true
		}
	}
	return nil, false
}

func (c Columns) Values() []any {
	v := make([]any, 0, len(c.cs))
	for i := range c.cs {
		v = append(v, c.cs[i].val)
	}
	return v
}

func (c Columns) Map() map[string]any {
	m := make(map[string]any)
	for i := range c.cs {
		m[c.cs[i].name] = c.cs[i].val
	}
	return m
}

func (c Columns) Set(name string, value any) {
	for i := range c.cs {
		if c.cs[i].name == name {
			c.cs[i].val = value
			return
		}
	}
}

func (c *Columns) Remove(name string) {
	for i := range c.cs {
		if c.cs[i].name == name {
			r := []column{}
			r = append(r, c.cs[:i]...)
			r = append(r, c.cs[i+1:]...)
			c.cs = r
			return
		}
	}
}
