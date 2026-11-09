package doris

import "strings"

// Factor shared key components out of the disjunction. In particular, a
// partition's rejected records share date/topic/partition (and often hash):
// checking offsets with IN avoids reparsing 128 copies of those predicates.
// Multiple varying columns must remain correlated, never independent IN lists.
// vals contains only literals already escaped by quote.
func visibilityPredicate(keys []string, vals [][]string) string {
	common, varying := []string{}, []int{}
	for col, k := range keys {
		same := true
		for _, row := range vals[1:] {
			if row[col] != vals[0][col] {
				same = false
				break
			}
		}
		if same {
			common = append(common, "`"+k+"`="+vals[0][col])
		} else {
			varying = append(varying, col)
		}
	}
	if len(varying) == 1 {
		col := varying[0]
		in := make([]string, len(vals))
		for i, row := range vals {
			in[i] = row[col]
		}
		common = append(common, "`"+keys[col]+"` IN ("+strings.Join(in, ",")+")")
	} else if len(varying) > 1 {
		alternatives := make([]string, len(vals))
		for i, row := range vals {
			parts := make([]string, len(varying))
			for j, col := range varying {
				parts[j] = "`" + keys[col] + "`=" + row[col]
			}
			alternatives[i] = "(" + strings.Join(parts, " AND ") + ")"
		}
		common = append(common, "("+strings.Join(alternatives, " OR ")+")")
	}
	return strings.Join(common, " AND ")
}
