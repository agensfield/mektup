package executor

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/agensfield/mektup/go/internal/doctor"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
)

// humanResult projects the complete machine result into concise, stable text.
// It must never mutate data: the same value is independently retained in the
// JSONL lifecycle event.
func humanResult(eventKind string, data any, cursor, subcommand string) string {
	switch eventKind {
	case "endpoint.list":
		if value, ok := data.([]endpoint.Endpoint); ok {
			return humanEndpointList(value)
		}
	case "endpoint.show", "endpoint.add":
		if value, ok := data.(endpoint.Endpoint); ok {
			verb := "endpoint"
			if eventKind == "endpoint.add" {
				verb = "added endpoint"
			}
			return verb + " " + humanEndpoint(value)
		}
	case "endpoint.remove":
		if value, ok := object(data)["selector"].(string); ok && value != "" {
			return "removed endpoint " + value
		}
	case "endpoint.check":
		item := object(data)
		name, _ := item["endpoint"].(string)
		if name == "" {
			name = "endpoint"
		}
		lines := []string{name + ": reachable"}
		for _, warning := range stringsFrom(item["warnings"]) {
			lines = append(lines, "warning: "+warning)
		}
		return strings.Join(lines, "\n")
	case "doctor":
		if value, ok := data.(doctor.Report); ok {
			return humanDoctor(value)
		}
	case "storage":
		return humanStorage(subcommand, data)
	case "thread.list", "thread.read", "thread.turns", "thread.items", "search":
		return humanCollection(eventKind, data, cursor)
	case "thread.start", "thread.resume", "thread.fork":
		return humanThreadLifecycle(eventKind, data)
	case "rpc":
		return humanRPC(data)
	}
	return humanFallback(eventKind, data, cursor)
}

func humanEndpointList(items []endpoint.Endpoint) string {
	if len(items) == 0 {
		return "no endpoints"
	}
	rows := make([][]string, 0, len(items))
	for _, item := range items {
		kind := string(item.Herdr)
		if item.Builtin {
			kind += ", builtin"
		}
		rows = append(rows, []string{item.Alias, item.ID, humanRoute(item.Route), kind})
	}
	return table([]string{"ALIAS", "ID", "ROUTE", "HERDR"}, rows)
}

func humanEndpoint(item endpoint.Endpoint) string {
	flags := []string{"herdr=" + string(item.Herdr)}
	if item.Builtin {
		flags = append(flags, "builtin")
	}
	return fmt.Sprintf("%s (%s) %s %s", item.Alias, item.ID, humanRoute(item.Route), strings.Join(flags, ", "))
}

func humanRoute(route endpoint.Route) string {
	switch route.Kind {
	case endpoint.RouteUnix:
		return "unix:" + route.UnixSocket
	case endpoint.RouteSSH:
		return "ssh:" + route.SSHHost
	default:
		return string(route.Kind)
	}
}

func humanDoctor(report doctor.Report) string {
	counts := map[doctor.Severity]int{}
	for _, finding := range report.Findings {
		counts[finding.Severity]++
	}
	state := "healthy"
	if counts[doctor.SeverityError] != 0 || counts[doctor.SeverityWarning] != 0 {
		state = "attention required"
	}
	header := fmt.Sprintf("doctor: %s (%d ok, %d notices, %d warnings, %d errors)", state,
		counts[doctor.SeverityOK], counts[doctor.SeverityNotice], counts[doctor.SeverityWarning], counts[doctor.SeverityError])
	rows := make([][]string, 0, len(report.Findings))
	for _, finding := range report.Findings {
		rows = append(rows, []string{strings.ToUpper(string(finding.Severity)), finding.ID, finding.Message, finding.Path})
	}
	parts := []string{header}
	if len(rows) != 0 {
		parts = append(parts, table([]string{"STATE", "CHECK", "DETAIL", "PATH"}, rows))
	}
	if report.Fix {
		repairs := make([][]string, 0, len(report.Repairs))
		for _, repair := range report.Repairs {
			state := "not applied"
			if repair.Applied {
				state = "applied"
			} else if repair.Error != "" {
				state = "failed: " + repair.Error
			}
			repairs = append(repairs, []string{repair.ID, state, firstNonEmptyText(repair.Detail, repair.Action)})
		}
		if len(repairs) == 0 {
			parts = append(parts, "repairs: none needed")
		} else {
			parts = append(parts, table([]string{"REPAIR", "RESULT", "DETAIL"}, repairs))
		}
	}
	return strings.Join(parts, "\n")
}

func humanStorage(subcommand string, data any) string {
	switch value := data.(type) {
	case journal.StorageStatus:
		lines := []string{
			"storage: " + value.DatabasePath,
			fmt.Sprintf("schema=%d sqlite=%s journal=%s database=%s wal=%s shm=%s", value.SchemaVersion, value.SQLiteVersion, value.JournalMode, humanBytes(value.DatabaseBytes), humanBytes(value.WALBytes), humanBytes(value.SHMBytes)),
			"records: " + counts(value.Counts),
			"operations: " + counts(value.OperationCounts),
			"replies: " + counts(value.ReplyCounts),
			fmt.Sprintf("retention: %d eligible before %s", value.RetentionEligible, humanTime(value.RetentionCutoff)),
		}
		return strings.Join(lines, "\n")
	case journal.StorageCheck:
		issues := append(append([]string(nil), value.ForeignKeyIssues...), value.RelationshipIssues...)
		if len(issues) == 0 && value.Integrity == "ok" {
			return fmt.Sprintf("storage check: ok (read-only=%t)", value.ReadOnly)
		}
		lines := []string{fmt.Sprintf("storage check: %s (read-only=%t)", value.Integrity, value.ReadOnly)}
		for _, issue := range issues {
			lines = append(lines, "issue: "+issue)
		}
		return strings.Join(lines, "\n")
	case journal.MaintenanceReceipt:
		mode := "applied"
		if value.DryRun {
			mode = "dry run"
		}
		rows := make([][]string, 0, len(value.Actions))
		for _, action := range value.Actions {
			result := "skipped"
			if action.Busy {
				result = "busy"
			} else if action.Error != "" {
				result = "failed: " + action.Error
			} else if action.Applied {
				result = "applied"
			} else if action.Attempted {
				result = "attempted"
			}
			rows = append(rows, []string{action.Kind, result, fmt.Sprintf("eligible=%d changed=%d", action.Eligible, action.Changed)})
		}
		return fmt.Sprintf("storage maintenance: %s; retention cutoff %s\n%s", mode, humanTime(value.RetentionCutoff), table([]string{"ACTION", "RESULT", "COUNTS"}, rows))
	case journal.VacuumReceipt:
		return fmt.Sprintf("storage vacuum: applied=%t %s → %s (%s)", value.Applied, humanBytes(value.BeforeBytes), humanBytes(value.AfterBytes), value.DatabasePath)
	}
	return humanFallback("storage "+subcommand, data, "")
}

func humanCollection(kind string, data any, cursor string) string {
	items := slice(data)
	if len(items) == 0 {
		return emptyCollection(kind)
	}
	var headers []string
	rows := make([][]string, 0, len(items))
	for _, raw := range items {
		item := object(raw)
		switch kind {
		case "thread.list":
			headers = []string{"THREAD", "STATUS", "MODEL", "NAME", "UPDATED"}
			rows = append(rows, []string{stringAt(item, "id"), nestedStatus(item["status"]), stringAt(item, "model"), compact(firstNonEmptyText(stringAt(item, "name"), stringAt(item, "preview")), 56), epochAt(item, "updatedAt")})
		case "thread.read":
			return humanThreadRead(item)
		case "thread.turns":
			headers = []string{"TURN", "STATUS", "ITEMS", "VIEW"}
			rows = append(rows, []string{stringAt(item, "id"), nestedStatus(item["status"]), fmt.Sprint(len(slice(item["items"]))), stringAt(item, "itemsView")})
		case "thread.items":
			headers = []string{"TURN", "ITEM", "TYPE", "CONTENT"}
			entry := object(item["item"])
			if len(entry) == 0 {
				entry = item
			}
			rows = append(rows, []string{stringAt(item, "turnId"), stringAt(entry, "id"), stringAt(entry, "type"), compact(itemText(entry), 72)})
		case "search":
			if thread := object(item["thread"]); len(thread) != 0 {
				headers = []string{"THREAD", "STATUS", "NAME", "MATCH"}
				rows = append(rows, []string{stringAt(thread, "id"), nestedStatus(thread["status"]), compact(stringAt(thread, "name"), 32), compact(stringAt(item, "snippet"), 72)})
			} else {
				headers = []string{"TURN", "ITEM", "MATCH"}
				rows = append(rows, []string{stringAt(item, "turnId"), stringAt(item, "itemId"), compact(stringAt(item, "snippet"), 96)})
			}
		}
	}
	output := table(headers, rows)
	if cursor != "" {
		output += "\nnext cursor: " + cursor
	}
	return output
}

func humanThreadRead(thread map[string]any) string {
	lines := []string{"thread " + stringAt(thread, "id")}
	for _, field := range []struct{ label, key string }{{"name", "name"}, {"status", "status"}, {"model", "model"}, {"cwd", "cwd"}, {"updated", "updatedAt"}} {
		value := stringAt(thread, field.key)
		if field.key == "status" {
			value = nestedStatus(thread[field.key])
		} else if field.key == "updatedAt" {
			value = epochAt(thread, field.key)
		}
		if value != "" {
			lines = append(lines, field.label+": "+value)
		}
	}
	turns := slice(thread["turns"])
	lines = append(lines, fmt.Sprintf("turns: %d", len(turns)))
	for _, raw := range turns {
		turn := object(raw)
		lines = append(lines, fmt.Sprintf("  turn %s status=%s", stringAt(turn, "id"), nestedStatus(turn["status"])))
		for _, itemRaw := range slice(turn["items"]) {
			item := object(itemRaw)
			lines = append(lines, fmt.Sprintf("    %s %s  %s", stringAt(item, "type"), stringAt(item, "id"), compact(itemText(item), 120)))
		}
	}
	return strings.Join(lines, "\n")
}

func humanThreadLifecycle(kind string, data any) string {
	item := object(data)
	verb := strings.TrimPrefix(kind, "thread.")
	parts := []string{"thread " + verb, stringAt(item, "threadId")}
	for _, key := range []string{"status", "model", "modelProvider", "cwd"} {
		if value := stringAt(item, key); value != "" {
			parts = append(parts, key+"="+value)
		}
	}
	return strings.Join(parts, " ")
}

func humanRPC(data any) string {
	item := object(data)
	line := fmt.Sprintf("rpc %s id=%v effect=%s", stringAt(item, "method"), item["id"], stringAt(item, "effectState"))
	if effects := stringsFrom(item["effects"]); len(effects) != 0 {
		line += " effects=" + strings.Join(effects, ",")
	}
	if artifact := object(item["artifact"]); len(artifact) != 0 {
		line += fmt.Sprintf("\nartifact: %s bytes=%v digest=%s", stringAt(artifact, "path"), artifact["bytes"], stringAt(artifact, "sha256"))
	}
	if result, ok := item["result"]; ok {
		line += "\nresult:\n" + indent(humanJSON(result), "  ")
	}
	return line
}

func humanFallback(kind string, data any, cursor string) string {
	output := kind + ":\n" + indent(humanJSON(data), "  ")
	if cursor != "" {
		output += "\nnext cursor: " + cursor
	}
	return output
}

func emptyCollection(kind string) string {
	switch kind {
	case "thread.list":
		return "no threads"
	case "thread.read":
		return "thread not found"
	case "thread.turns":
		return "no turns"
	case "thread.items":
		return "no items"
	case "search":
		return "no matches"
	default:
		return "no results"
	}
}

func table(headers []string, rows [][]string) string {
	if len(rows) == 0 {
		return ""
	}
	var buffer strings.Builder
	writer := tabwriter.NewWriter(&buffer, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(writer, strings.Join(headers, "\t"))
	for _, row := range rows {
		_, _ = fmt.Fprintln(writer, strings.Join(row, "\t"))
	}
	_ = writer.Flush()
	return strings.TrimRight(buffer.String(), "\n")
}

func humanJSON(value any) string {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(encoded)
}

func object(value any) map[string]any {
	if result, ok := value.(map[string]any); ok {
		return result
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return map[string]any{}
	}
	var result map[string]any
	if json.Unmarshal(encoded, &result) != nil {
		return map[string]any{}
	}
	return result
}

func slice(value any) []any {
	if result, ok := value.([]any); ok {
		return result
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var result []any
	if json.Unmarshal(encoded, &result) != nil {
		return nil
	}
	return result
}

func stringAt(item map[string]any, key string) string {
	value, _ := item[key].(string)
	return value
}

func stringsFrom(value any) []string {
	items := slice(value)
	result := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok && text != "" {
			result = append(result, text)
		}
	}
	return result
}

func nestedStatus(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	item := object(value)
	if text := stringAt(item, "type"); text != "" {
		if flags := stringsFrom(item["activeFlags"]); len(flags) != 0 {
			return text + " (" + strings.Join(flags, ",") + ")"
		}
		return text
	}
	return ""
}

func itemText(item map[string]any) string {
	for _, key := range []string{"text", "aggregatedOutput", "command", "snippet"} {
		if value := stringAt(item, key); value != "" {
			return value
		}
	}
	parts := make([]string, 0)
	for _, raw := range slice(item["content"]) {
		if text := stringAt(object(raw), "text"); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, " ")
}

func compact(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	if limit <= 0 || utf8.RuneCountInString(value) <= limit {
		return value
	}
	runes := []rune(value)
	return string(runes[:limit-1]) + "…"
}

func counts(values map[string]int64) string {
	if len(values) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, values[key]))
	}
	return strings.Join(parts, ", ")
}

func humanBytes(value int64) string {
	const unit = int64(1024)
	if value < unit {
		return fmt.Sprintf("%d B", value)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	amount := float64(value)
	index := -1
	for amount >= float64(unit) && index+1 < len(units) {
		amount /= float64(unit)
		index++
	}
	return fmt.Sprintf("%.1f %s", amount, units[index])
}

func humanTime(value time.Time) string {
	if value.IsZero() {
		return "unknown"
	}
	return value.UTC().Format(time.RFC3339)
}

func epochAt(item map[string]any, key string) string {
	switch value := item[key].(type) {
	case string:
		return value
	case float64:
		return time.Unix(int64(value), 0).UTC().Format(time.RFC3339)
	case json.Number:
		seconds, _ := value.Int64()
		return time.Unix(seconds, 0).UTC().Format(time.RFC3339)
	default:
		return ""
	}
}

func indent(value, prefix string) string {
	return prefix + strings.ReplaceAll(value, "\n", "\n"+prefix)
}

func firstNonEmptyText(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
