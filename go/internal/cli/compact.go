package cli

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	CompactDefaultLimit   = 10
	CompactMaxLimit       = 25
	CompactMaxOutputBytes = 128 << 10
	CompactRPCInlineBytes = 48 << 10
	compactPreviewRunes   = 512
	compactPreviewBytes   = 2048
)

func resolveCompactInvocation(inv *Invocation) *Error {
	if inv == nil {
		return nil
	}
	if inv.Command == "--skill" || inv.Command == "completion" {
		if inv.Global.Compact {
			return usageError("--compact is not valid for exact-text skill or completion output")
		}
		inv.Resolved.Compact = false
		return nil
	}
	if inv.Global.Help || inv.Command == "help" || inv.Command == "docs" || inv.Command == "version" || inv.Command == "" {
		inv.Resolved.Compact = false
		return nil
	}
	exact := has(*inv, "portable") || has(*inv, "content") || (has(*inv, "view") && inv.Option("view") == "full")
	if exact && inv.Global.Compact {
		return usageError("--compact cannot be combined with --portable, --content, or --view full")
	}
	if exact {
		inv.Resolved.Compact = false
	}
	return nil
}

func applyCompactDefaults(inv *Invocation) {
	if inv == nil || !inv.Resolved.Compact {
		return
	}
	key := strings.Join(inv.Path, " ")
	switch key {
	case "search", "thread list", "thread turns", "thread items", "receipt list":
		if len(inv.Options["limit"]) == 0 {
			inv.Options["limit"] = []string{fmt.Sprint(CompactDefaultLimit)}
		} else {
			value := optionInteger(*inv, "limit")
			inv.Resolved.CompactRequestedLimit = &value
			if value == 0 {
				inv.Options["limit"] = []string{fmt.Sprint(CompactDefaultLimit)}
			}
		}
	case "inspect":
		if len(inv.Options["receipts"]) == 0 {
			inv.Options["receipts"] = []string{fmt.Sprint(CompactDefaultLimit)}
		} else {
			value := optionInteger(*inv, "receipts")
			inv.Resolved.CompactRequestedReceipts = &value
		}
	}
}

func compactLifecycleEvent(inv Invocation, event map[string]any) map[string]any {
	if !inv.Resolved.Compact || event == nil {
		return event
	}
	data := objectMap(event["data"])
	endpointID, _ := data["endpointId"].(string)
	threadID, _ := data["threadId"].(string)
	event["presentation"] = "compact"
	event["data"] = data
	name, _ := event["event"].(string)
	switch name {
	case "thread.list.completed":
		data["data"] = projectSlice(data["data"], func(value any) any { return compactThreadAt(value, endpointID) })
		setCompactPage(data, inv)
	case "thread.read.completed":
		data["data"] = projectSlice(data["data"], func(value any) any { return compactThreadAt(value, endpointID) })
	case "thread.turns.completed":
		data["data"] = projectSlice(data["data"], func(value any) any { return compactTurnAt(value, endpointID, threadID) })
		setCompactPage(data, inv)
	case "thread.items.completed":
		data["data"] = projectSlice(data["data"], func(value any) any { return compactItemEntryAt(value, endpointID, threadID) })
		setCompactPage(data, inv)
	case "search.completed":
		if kind, _ := data["resultKind"].(string); kind == "message" {
			data["data"] = projectSlice(data["data"], func(value any) any { return compactOccurrenceAt(value, endpointID, threadID) })
		} else {
			data["data"] = projectSlice(data["data"], func(value any) any { return compactSearchResultAt(value, endpointID) })
		}
		setCompactPage(data, inv)
	case "inspect.completed":
		data["target"] = compactTarget(data["target"])
		data["receipts"] = projectSlice(data["receipts"], compactReceipt)
		data["blockers"] = projectSlice(data["blockers"], compactBlocker)
		setInspectReceiptsPage(data, inv)
		setCompactCount(data, "blockers")
	case "receipt.list":
		data["receipts"] = projectSlice(data["receipts"], compactReceipt)
		setCompactPageFor(data, inv, "receipts")
	case "endpoint.list.completed":
		data["result"] = projectSlice(data["result"], compactEndpoint)
		data["count"] = len(anySlice(data["result"]))
	case "endpoint.show.completed", "endpoint.add.completed", "endpoint.check.completed":
		data["result"] = compactEndpoint(data["result"])
	}
	if receipt, ok := data["receipt"]; ok {
		data["receipt"] = compactReceipt(receipt)
	}
	if warnings, ok := event["warnings"]; ok {
		raw := anySlice(warnings)
		projected, truncated := compactWarnings(raw)
		event["warningCount"] = len(raw)
		event["warnings"] = projected
		if truncated {
			event["warningsTruncated"] = true
		}
	}
	return event
}

func compactEndpoint(value any) any {
	in := objectMap(value)
	out := pick(in, "id", "alias", "route", "transport", "herdr", "builtin", "healthy", "status", "serverVersion", "compatibility")
	if len(out) == 0 {
		return value
	}
	return out
}

func compactEncodedSize(value any) int {
	encoded, err := json.Marshal(value)
	if err != nil {
		return CompactMaxOutputBytes + 1
	}
	return len(encoded) + 1
}

func setCompactPage(data map[string]any, inv Invocation) {
	setCompactPageFor(data, inv, "data")
}

func setCompactPageFor(data map[string]any, inv Invocation, field string) {
	returned := len(anySlice(data[field]))
	limit := optionInteger(inv, "limit")
	var requested any
	if inv.Resolved.CompactRequestedLimit != nil {
		requested = *inv.Resolved.CompactRequestedLimit
	}
	if field == "receipts" && limit == 0 {
		limit = optionInteger(inv, "receipts")
		if inv.Resolved.CompactRequestedReceipts != nil {
			requested = *inv.Resolved.CompactRequestedReceipts
		}
	}
	if limit == 0 {
		limit = CompactDefaultLimit
	}
	next, _ := data["nextCursor"].(string)
	if field == "receipts" {
		if cursor, ok := data["receiptsNextCursor"].(string); ok {
			next = cursor
		}
	}
	data["count"] = returned
	data["requestedLimit"] = requested
	data["effectiveLimit"] = limit
	data["hasMore"] = next != ""
	if next == "" {
		data["nextCursor"] = nil
	}
}

func setInspectReceiptsPage(data map[string]any, inv Invocation) {
	returned := len(anySlice(data["receipts"]))
	limit := optionInteger(inv, "receipts")
	var requested any
	if inv.Resolved.CompactRequestedReceipts != nil {
		requested = *inv.Resolved.CompactRequestedReceipts
	}
	next, _ := data["receiptsNextCursor"].(string)
	page := map[string]any{"count": returned, "requestedLimit": requested, "effectiveLimit": limit, "hasMore": next != ""}
	if next == "" {
		page["nextCursor"] = nil
	} else {
		page["nextCursor"] = next
	}
	data["receiptsPage"] = page
	delete(data, "receiptsNextCursor")
}

func setCompactCount(data map[string]any, field string) {
	data[field+"Returned"] = len(anySlice(data[field]))
}

func optionInteger(inv Invocation, name string) int {
	var value int
	_, _ = fmt.Sscan(inv.Option(name), &value)
	return value
}

func projectSlice(value any, project func(any) any) []any {
	items := anySlice(value)
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, project(item))
	}
	return out
}

func compactThread(value any) any {
	return compactThreadAt(value, "")
}

func compactThreadAt(value any, endpointID string) any {
	if id, ok := value.(string); ok {
		out := map[string]any{"id": id}
		addThreadLocator(out, endpointID, id)
		return out
	}
	in := objectMap(value)
	out := pick(in, "id", "status", "model", "createdAt", "updatedAt", "cwd", "source", "projectId", "parentThreadId", "ancestorThreadId", "ephemeral", "archived")
	addThreadLocator(out, endpointID, stringValue(in["id"]))
	copyPreview(out, "name", in["name"])
	copyPreview(out, "preview", in["preview"])
	if turns := anySlice(in["turns"]); len(turns) != 0 {
		out["turnCount"] = len(turns)
	}
	return out
}

func compactTurn(value any) any {
	return compactTurnAt(value, "", "")
}

func compactTurnAt(value any, endpointID, threadID string) any {
	in := objectMap(value)
	out := pick(in, "id", "status", "itemsView", "createdAt", "updatedAt")
	out["itemCount"] = len(anySlice(in["items"]))
	addHistoryLocator(out, endpointID, threadID, stringValue(in["id"]), "")
	return out
}

func compactItemEntry(value any) any {
	return compactItemEntryAt(value, "", "")
}

func compactItemEntryAt(value any, endpointID, threadID string) any {
	in := objectMap(value)
	item := objectMap(in["item"])
	if len(item) == 0 {
		item = in
	}
	out := map[string]any{}
	copyKnown(out, in, "turnId")
	for _, key := range []string{"id", "type", "phase", "status", "clientId"} {
		copyKnown(out, item, key)
	}
	text := itemTextValue(item)
	if text != "" {
		copyPreview(out, "textPreview", text)
	}
	if content := anySlice(item["content"]); len(content) != 0 {
		out["contentCount"] = len(content)
	}
	addHistoryLocator(out, endpointID, threadID, stringValue(in["turnId"]), stringValue(item["id"]))
	return out
}

func compactSearchResult(value any) any {
	return compactSearchResultAt(value, "")
}

func compactSearchResultAt(value any, endpointID string) any {
	in := objectMap(value)
	return map[string]any{"thread": compactThreadAt(in["thread"], endpointID), "snippet": previewValue(in["snippet"])}
}

func compactOccurrence(value any) any {
	return compactOccurrenceAt(value, "", "")
}

func compactOccurrenceAt(value any, endpointID, threadID string) any {
	in := objectMap(value)
	out := pick(in, "turnId", "itemId", "turnCursor")
	out["snippet"] = previewValue(in["snippet"])
	if matchRange, ok := in["snippetMatchRange"]; ok && matchRange != nil {
		out["snippetMatchRange"] = matchRange
		out["rangeBasis"] = "original-utf16"
	}
	addHistoryLocator(out, endpointID, threadID, stringValue(in["turnId"]), stringValue(in["itemId"]))
	return out
}

func addThreadLocator(out map[string]any, endpointID, threadID string) {
	if endpointID != "" {
		out["endpointId"] = endpointID
	}
	if endpointID != "" && threadID != "" {
		out["uri"] = "codex://" + endpointID + "/thread/" + threadID
	}
}

func addHistoryLocator(out map[string]any, endpointID, threadID, turnID, itemID string) {
	addThreadLocator(out, endpointID, threadID)
	locator := map[string]any{}
	copyNonempty := func(key, value string) {
		if value != "" {
			locator[key] = value
		}
	}
	copyNonempty("endpointId", endpointID)
	copyNonempty("threadId", threadID)
	copyNonempty("turnId", turnID)
	copyNonempty("itemId", itemID)
	if len(locator) != 0 {
		out["historyLocator"] = locator
	}
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func compactTarget(value any) any {
	in := objectMap(value)
	out := pick(in, "EndpointID", "EndpointAlias", "Transport", "ServerVersion", "Compatibility", "ThreadID", "Requested", "Resolved", "Loaded", "Status", "ActiveTurnID")
	if len(out) == 0 {
		out = pick(in, "endpointId", "endpointAlias", "transport", "serverVersion", "compatibility", "threadId", "requested", "resolved", "loaded", "status", "activeTurnId")
	}
	if evidence := objectMap(firstPresent(in, "HerdrEvidence", "herdrEvidence")); len(evidence) != 0 {
		out["herdrEvidence"] = pick(evidence, "name", "workspaceId", "tabId", "paneId", "codexThreadId", "status")
	}
	return out
}

func compactReceipt(value any) any {
	in := objectMap(value)
	out := pick(in, "receiptId", "operationId", "operation", "state", "createdAt", "updatedAt", "contentRef")
	out["schema"] = "mektup/receipt-summary/v1"
	out["projection"] = "receipt-summary"
	out["canonical"] = false
	out["source"] = compactIdentity(in["source"])
	out["target"] = compactIdentity(in["target"])
	if manual := objectMap(in["manualResolution"]); len(manual) != 0 {
		out["manualResolutionKind"] = "assertion"
		projected := pick(manual, "assertion", "actor", "timestamp", "evidenceRef", "presentationMode")
		copyPreview(projected, "reason", manual["reason"])
		out["manualResolution"] = projected
	}
	message := objectMap(in["message"])
	out["message"] = pick(message, "messageId", "inReplyTo", "kind", "replyRequested", "payloadBytes", "payloadSha256", "clientMessageId", "turnId", "acceptedAt", "replyAt")
	evidence := anySlice(in["evidence"])
	out["evidenceCount"] = len(evidence)
	projectedEvidence, evidenceTruncated := compactEvidenceRows(evidence)
	out["evidence"] = projectedEvidence
	if len(evidence) != 0 {
		out["latestEvidence"] = compactEvidence(evidence[len(evidence)-1])
	}
	if causal := compactCausalEvidence(evidence); len(causal) != 0 {
		out["causalEvidence"] = causal
	}
	if evidenceTruncated {
		out["evidenceTruncated"] = true
	}
	warnings := anySlice(in["warnings"])
	out["warningCount"] = len(warnings)
	projectedWarnings, warningsTruncated := compactWarnings(warnings)
	out["warnings"] = projectedWarnings
	if warningsTruncated {
		out["warningsTruncated"] = true
	}
	return out
}

func compactCausalEvidence(evidence []any) map[string]any {
	wanted := map[string]bool{
		"not_sent": true, "dispatch_started": true, "rejected": true, "accepted": true,
		"outcome_unknown": true, "reply_outcome_unknown": true, "reply_accepted": true,
		"reply_observed": true, "manually_resolved": true,
	}
	out := map[string]any{}
	for _, raw := range evidence {
		state, _ := objectMap(raw)["state"].(string)
		if wanted[state] {
			out[state] = compactEvidence(raw)
		}
	}
	return out
}

func compactEvidenceRows(evidence []any) ([]any, bool) {
	if len(evidence) <= CompactMaxLimit {
		return projectSlice(evidence, compactEvidence), false
	}
	selected := make([]any, 0, CompactMaxLimit)
	selected = append(selected, evidence[:CompactMaxLimit-1]...)
	selected = append(selected, evidence[len(evidence)-1])
	return projectSlice(selected, compactEvidence), true
}

func compactEvidence(value any) any {
	in := objectMap(value)
	out := pick(in, "state", "at", "kind", "reference")
	details := objectMap(in["details"])
	selected := pick(details,
		"effectState", "phase", "writeEvidence",
		"custodyRoute", "custodyStoreId",
		"replyMessageId", "replyStatus", "replyErrorCode", "replyDigest", "replyBodyBytes",
		"bodySha256", "bodyBytes", "commitSeq", "eventSeq", "nativeItemId", "winnerReplyId", "winnerCommitSeq",
		"endpointId", "controlRoute", "provenance", "turnId", "itemId")
	if len(selected) != 0 {
		out["details"] = selected
	}
	return out
}

func compactIdentity(value any) any {
	in := objectMap(value)
	return pick(in, "endpointId", "alias", "transport", "serverVersion", "compatibility", "threadId", "requested", "resolved")
}

func compactBlocker(value any) any {
	in := objectMap(value)
	out := map[string]any{}
	for _, pair := range [][2]string{
		{"method", "Method"}, {"correlationId", "CorrelationID"}, {"generation", "Generation"},
		{"firstSeen", "FirstSeen"}, {"lastSeen", "LastSeen"}, {"resolvedAt", "ResolvedAt"},
		{"endpointId", "EndpointID"}, {"threadId", "ThreadID"}, {"turnId", "TurnID"},
		{"itemId", "ItemID"}, {"operationId", "OperationID"}, {"messageId", "MessageID"},
	} {
		if raw := firstPresent(in, pair[0], pair[1]); raw != nil {
			out[pair[0]] = raw
		}
	}
	return out
}

func compactWarning(value any) any {
	in := objectMap(value)
	out := pick(in, "code")
	if message, ok := in["message"].(string); ok && message != "" {
		preview := previewValue(message)
		out["message"] = objectMap(preview)["text"]
		out["messagePreview"] = preview
	}
	return out
}

func compactWarnings(warnings []any) ([]any, bool) {
	limit := len(warnings)
	if limit > CompactMaxLimit {
		limit = CompactMaxLimit
	}
	return projectSlice(warnings[:limit], compactWarning), limit != len(warnings)
}

func previewValue(value any) any {
	text, _ := value.(string)
	preview, sourceChars, sourceBytes, chars, bytes, truncated := boundedPreview(text)
	return map[string]any{"text": preview, "sourceChars": sourceChars, "sourceBytes": sourceBytes, "chars": chars, "bytes": bytes, "truncated": truncated}
}

func copyPreview(out map[string]any, key string, value any) {
	text, ok := value.(string)
	if !ok || text == "" {
		return
	}
	out[key] = previewValue(text)
}

func boundedPreview(value string) (string, int, int, int, int, bool) {
	sourceChars, sourceBytes := utf8.RuneCountInString(value), len(value)
	if sourceChars <= compactPreviewRunes && sourceBytes <= compactPreviewBytes {
		return value, sourceChars, sourceBytes, sourceChars, sourceBytes, false
	}
	runes := []rune(value)
	if len(runes) > compactPreviewRunes {
		runes = runes[:compactPreviewRunes]
	}
	for len(runes) > 0 && len(string(runes)) > compactPreviewBytes {
		runes = runes[:len(runes)-1]
	}
	preview := string(runes)
	return preview, sourceChars, sourceBytes, utf8.RuneCountInString(preview), len(preview), true
}

func itemTextValue(item map[string]any) string {
	for _, key := range []string{"text", "aggregatedOutput", "command", "snippet"} {
		if value, ok := item[key].(string); ok && value != "" {
			return value
		}
	}
	parts := make([]string, 0)
	for _, raw := range anySlice(item["content"]) {
		if text, ok := objectMap(raw)["text"].(string); ok && text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, " ")
}

func pick(in map[string]any, keys ...string) map[string]any {
	out := make(map[string]any)
	for _, key := range keys {
		copyKnown(out, in, key)
	}
	return out
}

func copyKnown(out, in map[string]any, key string) {
	if value, ok := in[key]; ok && value != nil {
		out[key] = value
	}
}

func firstPresent(in map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := in[key]; ok {
			return value
		}
	}
	return nil
}

func objectMap(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return map[string]any{}
	}
	out, err := decodeExactObject(encoded)
	if err != nil {
		return map[string]any{}
	}
	return out
}

func anySlice(value any) []any {
	if typed, ok := value.([]any); ok {
		return typed
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	var out []any
	if decoder.Decode(&out) != nil {
		return nil
	}
	return out
}
