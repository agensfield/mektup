package journal

import (
	"context"
	"fmt"
	"strings"
)

type schemaColumnSpec struct {
	name         string
	typ          string
	notNull      int
	defaultValue *string
	pk           int
}

type schemaUniqueSpec struct {
	columns    []string
	strictOnly bool
}
type schemaIndexSpec struct {
	name    string
	columns []string
	unique  bool
}
type schemaForeignKeySpec struct {
	from, table, to, onDelete string
}
type schemaTableSpec struct {
	name       string
	columns    []schemaColumnSpec
	uniques    []schemaUniqueSpec
	indexes    []schemaIndexSpec
	foreignKey []schemaForeignKeySpec
	checks     []string
}

func textDefault(value string) *string { return &value }

func currentSchemaManifest() []schemaTableSpec {
	text := func(name string, notNull, pk int, def *string) schemaColumnSpec {
		return schemaColumnSpec{name: name, typ: "TEXT", notNull: notNull, pk: pk, defaultValue: def}
	}
	integer := func(name string, notNull, pk int, def *string) schemaColumnSpec {
		return schemaColumnSpec{name: name, typ: "INTEGER", notNull: notNull, pk: pk, defaultValue: def}
	}
	return []schemaTableSpec{
		{name: "meta", columns: []schemaColumnSpec{text("key", 0, 1, nil), text("value", 1, 0, nil)}},
		{name: "store_id_aliases", columns: []schemaColumnSpec{text("alias", 0, 1, nil), text("store_id", 1, 0, nil), integer("created_at", 1, 0, nil)}},
		{name: "operations", columns: []schemaColumnSpec{
			text("operation_id", 0, 1, nil), text("message_id", 1, 0, nil), text("source_route", 1, 0, nil), text("target_route", 1, 0, nil), text("semantics", 1, 0, nil), text("source_endpoint_id", 1, 0, textDefault("''")), text("target_endpoint_id", 1, 0, textDefault("''")), text("reply_route", 1, 0, textDefault("''")), text("reply_endpoint_id", 1, 0, textDefault("''")), text("reply_thread_id", 1, 0, textDefault("''")), text("custody_route", 1, 0, textDefault("''")), text("custody_store_id", 1, 0, textDefault("''")), text("digest", 1, 0, nil), integer("body_size", 1, 0, nil), text("state", 1, 0, nil), integer("created_at", 1, 0, nil), integer("updated_at", 1, 0, nil), integer("dispatch_started_at", 0, 0, nil), integer("terminal_at", 0, 0, nil), text("error_code", 1, 0, textDefault("''")),
		}, uniques: []schemaUniqueSpec{{columns: []string{"message_id"}}}, checks: []string{"body_size >= 0"}},
		{name: "attempts", columns: []schemaColumnSpec{text("operation_id", 0, 1, nil), text("state", 1, 0, nil), integer("created_at", 1, 0, nil), integer("updated_at", 1, 0, nil), text("owner", 1, 0, nil), text("token", 1, 0, nil), integer("lease_until", 1, 0, nil)}, foreignKey: []schemaForeignKeySpec{{from: "operation_id", table: "operations", to: "operation_id", onDelete: "CASCADE"}}},
		{name: "reply_claims", columns: []schemaColumnSpec{text("reply_id", 0, 1, nil), text("original_id", 1, 0, nil), text("digest", 1, 0, nil), integer("body_size", 1, 0, nil), text("status", 1, 0, nil), text("reply_route", 1, 0, nil), text("custody_route", 1, 0, nil), text("custody_store_id", 1, 0, textDefault("''")), text("owner", 1, 0, nil), text("token", 1, 0, nil), integer("lease_until", 1, 0, nil), text("state", 1, 0, nil), integer("created_at", 1, 0, nil), integer("updated_at", 1, 0, nil), integer("accepted_at", 0, 0, nil), integer("commit_seq", 0, 0, nil), text("error_code", 1, 0, textDefault("''")), text("reply_error_code", 1, 0, textDefault("''"))}, uniques: []schemaUniqueSpec{{columns: []string{"reply_id"}}, {columns: []string{"reply_id", "original_id"}, strictOnly: true}}, checks: []string{"body_size >= 0", "status IN ('success','error')"}},
		{name: "reply_winners", columns: []schemaColumnSpec{text("original_id", 0, 1, nil), text("reply_id", 1, 0, nil), integer("committed_at", 1, 0, nil), integer("commit_seq", 1, 0, nil)}, foreignKey: []schemaForeignKeySpec{{from: "reply_id", table: "reply_claims", to: "reply_id", onDelete: "NO ACTION"}}},
		{name: "observations", columns: []schemaColumnSpec{text("reply_id", 0, 1, nil), text("native_item_id", 1, 0, nil), integer("observed_at", 1, 0, nil), text("digest", 1, 0, nil), text("endpoint_id", 1, 0, textDefault("''")), text("control_route", 1, 0, textDefault("''"))}, foreignKey: []schemaForeignKeySpec{{from: "reply_id", table: "reply_claims", to: "reply_id", onDelete: "CASCADE"}}},
		{name: "events", columns: []schemaColumnSpec{integer("seq", 0, 1, nil), text("kind", 1, 0, nil), text("operation_id", 0, 0, nil), text("reply_id", 0, 0, nil), text("state", 1, 0, nil), integer("at", 1, 0, nil)}, checks: nil},
		{name: "manual_resolutions", columns: []schemaColumnSpec{text("operation_id", 0, 1, nil), text("assertion", 1, 0, nil), text("actor", 1, 0, nil), text("reason", 1, 0, nil), text("evidence_ref", 1, 0, nil), text("presentation", 1, 0, textDefault("''")), integer("resolved_at", 1, 0, nil)}, foreignKey: []schemaForeignKeySpec{{from: "operation_id", table: "operations", to: "operation_id", onDelete: "CASCADE"}}},
		{name: "operation_acceptances", columns: []schemaColumnSpec{text("operation_id", 0, 1, nil), text("evidence_ref", 1, 0, nil), integer("recorded_at", 1, 0, nil)}, foreignKey: []schemaForeignKeySpec{{from: "operation_id", table: "operations", to: "operation_id", onDelete: "CASCADE"}}},
		{name: "reply_acceptances", columns: []schemaColumnSpec{text("reply_id", 0, 1, nil), text("evidence_ref", 1, 0, nil), integer("recorded_at", 1, 0, nil)}, foreignKey: []schemaForeignKeySpec{{from: "reply_id", table: "reply_claims", to: "reply_id", onDelete: "CASCADE"}}},
		{name: "receipts", columns: []schemaColumnSpec{text("receipt_id", 0, 1, nil), text("operation_id", 1, 0, nil), text("message_id", 1, 0, nil), text("state", 1, 0, nil), integer("created_at", 1, 0, nil), integer("updated_at", 1, 0, nil), text("source_endpoint_id", 1, 0, textDefault("''")), text("source_thread_id", 1, 0, textDefault("''")), text("target_endpoint_id", 1, 0, textDefault("''")), text("target_thread_id", 1, 0, textDefault("''")), text("document", 1, 0, nil)}, indexes: []schemaIndexSpec{{name: "receipts_operation", columns: []string{"operation_id"}}, {name: "receipts_message", columns: []string{"message_id"}}}},
		{name: "blockers", columns: []schemaColumnSpec{text("method", 1, 3, nil), text("correlation_id", 1, 4, nil), text("generation", 1, 2, textDefault("''")), integer("first_seen", 1, 0, nil), integer("last_seen", 1, 0, nil), integer("resolved_at", 0, 0, nil), text("endpoint_id", 1, 1, textDefault("''")), text("thread_id", 1, 0, textDefault("''")), text("turn_id", 1, 0, textDefault("''")), text("item_id", 1, 0, textDefault("''")), text("operation_id", 1, 0, textDefault("''")), text("message_id", 1, 0, textDefault("''"))}, indexes: []schemaIndexSpec{{name: "blockers_thread", columns: []string{"thread_id", "last_seen"}}}},
	}
}

func validateCurrentV8Schema(ctx context.Context, queryer schemaQueryer, strictChecks ...bool) error {
	checkConstraints := true
	if len(strictChecks) > 0 {
		checkConstraints = strictChecks[0]
	}
	for _, table := range currentSchemaManifest() {
		var count int
		if err := queryer.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table.name).Scan(&count); err != nil {
			return fmt.Errorf("%w: inspect table %s: %v", ErrCorrupt, table.name, err)
		}
		if count != 1 {
			return fmt.Errorf("%w: required current table %s is missing", ErrCorrupt, table.name)
		}
		if err := validateColumns(ctx, queryer, table); err != nil {
			return err
		}
		if err := validateUniques(ctx, queryer, table, checkConstraints); err != nil {
			return err
		}
		if err := validateIndexes(ctx, queryer, table); err != nil {
			return err
		}
		if checkConstraints {
			if err := validateForeignKeys(ctx, queryer, table); err != nil {
				return err
			}
		}
		if checkConstraints {
			if err := validateChecks(ctx, queryer, table); err != nil {
				return err
			}
		}
	}
	return nil
}

type observedColumn struct {
	typ         string
	notNull, pk int
	def         any
}

func validateColumns(ctx context.Context, q schemaQueryer, spec schemaTableSpec) error {
	rows, err := q.QueryContext(ctx, "PRAGMA table_info('"+spec.name+"')")
	if err != nil {
		return fmt.Errorf("%w: inspect columns %s: %v", ErrCorrupt, spec.name, err)
	}
	columns := map[string]observedColumn{}
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var def any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &def, &pk); err != nil {
			rows.Close()
			return fmt.Errorf("%w: inspect columns %s: %v", ErrCorrupt, spec.name, err)
		}
		columns[name] = observedColumn{typ: strings.ToUpper(strings.TrimSpace(typ)), notNull: notNull, pk: pk, def: def}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("%w: inspect columns %s: %v", ErrCorrupt, spec.name, err)
	}
	rows.Close()
	for _, want := range spec.columns {
		got, ok := columns[want.name]
		if !ok || got.typ != want.typ || got.notNull != want.notNull || got.pk != want.pk || want.defaultValue != nil && normalizeSchemaDefault(got.def) != *want.defaultValue {
			return fmt.Errorf("%w: malformed current column %s.%s", ErrCorrupt, spec.name, want.name)
		}
	}
	return nil
}

func normalizeSchemaDefault(v any) string { return strings.TrimSpace(fmt.Sprint(v)) }

func validateUniques(ctx context.Context, q schemaQueryer, spec schemaTableSpec, strict bool) error {
	for _, want := range spec.uniques {
		if want.strictOnly && !strict {
			continue
		}
		if !hasIndexColumns(ctx, q, spec.name, want.columns, true) {
			return fmt.Errorf("%w: missing unique constraint %s(%s)", ErrCorrupt, spec.name, strings.Join(want.columns, ","))
		}
	}
	return nil
}

func validateIndexes(ctx context.Context, q schemaQueryer, spec schemaTableSpec) error {
	for _, want := range spec.indexes {
		if !hasNamedIndex(ctx, q, spec.name, want) {
			return fmt.Errorf("%w: missing index %s", ErrCorrupt, want.name)
		}
	}
	return nil
}

func hasNamedIndex(ctx context.Context, q schemaQueryer, table string, want schemaIndexSpec) bool {
	rows, err := q.QueryContext(ctx, "PRAGMA index_list('"+table+"')")
	if err != nil {
		return false
	}
	found := false
	for rows.Next() {
		var seq int
		var name string
		var unique, partial int
		var origin string
		if rows.Scan(&seq, &name, &unique, &origin, &partial) != nil || name != want.name {
			continue
		}
		found = (unique == 1) == want.unique && partial == 0
	}
	rows.Close()
	return found && indexColumnsEqual(ctx, q, want.name, want.columns)
}

func hasIndexColumns(ctx context.Context, q schemaQueryer, table string, columns []string, unique bool) bool {
	rows, err := q.QueryContext(ctx, "PRAGMA index_list('"+table+"')")
	if err != nil {
		return false
	}
	names := []string{}
	for rows.Next() {
		var seq int
		var name string
		var isUnique, partial int
		var origin string
		if rows.Scan(&seq, &name, &isUnique, &origin, &partial) != nil {
			continue
		}
		if (isUnique == 1) != unique || partial != 0 {
			continue
		}
		names = append(names, name)
	}
	rows.Close()
	for _, name := range names {
		if indexColumnsEqual(ctx, q, name, columns) {
			return true
		}
	}
	return false
}

func indexColumnsEqual(ctx context.Context, q schemaQueryer, name string, columns []string) bool {
	rows, err := q.QueryContext(ctx, "PRAGMA index_info('"+name+"')")
	if err != nil {
		return false
	}
	defer rows.Close()
	got := []string{}
	for rows.Next() {
		var seq, cid int
		var column string
		if rows.Scan(&seq, &cid, &column) != nil {
			return false
		}
		got = append(got, column)
	}
	return rows.Err() == nil && strings.Join(got, "\x00") == strings.Join(columns, "\x00")
}

func validateForeignKeys(ctx context.Context, q schemaQueryer, spec schemaTableSpec) error {
	for _, want := range spec.foreignKey {
		rows, err := q.QueryContext(ctx, "PRAGMA foreign_key_list('"+spec.name+"')")
		if err != nil {
			return fmt.Errorf("%w: inspect foreign keys %s: %v", ErrCorrupt, spec.name, err)
		}
		found := false
		for rows.Next() {
			var id, seq int
			var table, from, to, onUpdate, onDelete, match string
			if rows.Scan(&id, &seq, &table, &from, &to, &onUpdate, &onDelete, &match) != nil {
				continue
			}
			if table == want.table && from == want.from && to == want.to && strings.EqualFold(onDelete, want.onDelete) {
				found = true
			}
		}
		rows.Close()
		if !found {
			return fmt.Errorf("%w: missing foreign key %s.%s", ErrCorrupt, spec.name, want.from)
		}
	}
	return nil
}

func validateChecks(ctx context.Context, q schemaQueryer, spec schemaTableSpec) error {
	if len(spec.checks) == 0 {
		return nil
	}
	var sqlText string
	if err := q.QueryRowContext(ctx, "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", spec.name).Scan(&sqlText); err != nil {
		return fmt.Errorf("%w: inspect checks %s: %v", ErrCorrupt, spec.name, err)
	}
	observed := extractCheckExpressions(sqlText)
	for _, check := range spec.checks {
		if _, ok := observed[normalizeCheckExpression(check)]; !ok {
			return fmt.Errorf("%w: missing check constraint %s.%s", ErrCorrupt, spec.name, check)
		}
	}
	return nil
}

func extractCheckExpressions(sqlText string) map[string]struct{} {
	checks := make(map[string]struct{})
	lower := strings.ToLower(sqlText)
	for offset := 0; offset < len(lower); {
		relative := strings.Index(lower[offset:], "check")
		if relative < 0 {
			break
		}
		start := offset + relative
		offset = start + len("check")
		if start > 0 && isSchemaIdentifierByte(lower[start-1]) || offset < len(lower) && isSchemaIdentifierByte(lower[offset]) {
			continue
		}
		for offset < len(lower) && (lower[offset] == ' ' || lower[offset] == '\t' || lower[offset] == '\r' || lower[offset] == '\n') {
			offset++
		}
		if offset >= len(lower) || lower[offset] != '(' {
			continue
		}
		expressionStart := offset + 1
		depth := 1
		quoted := false
		for offset++; offset < len(sqlText) && depth > 0; offset++ {
			switch sqlText[offset] {
			case '\'':
				if quoted && offset+1 < len(sqlText) && sqlText[offset+1] == '\'' {
					offset++
					continue
				}
				quoted = !quoted
			case '(':
				if !quoted {
					depth++
				}
			case ')':
				if !quoted {
					depth--
					if depth == 0 {
						checks[normalizeCheckExpression(sqlText[expressionStart:offset])] = struct{}{}
					}
				}
			}
		}
	}
	return checks
}

func normalizeCheckExpression(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), ""))
}

func isSchemaIdentifierByte(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
}
