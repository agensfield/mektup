package cli

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

type optionSpec struct {
	value  bool
	repeat bool
}

var optionSpecs = map[string]optionSpec{
	"json": {}, "human": {}, "compact": {}, "debug": {}, "audit": {}, "help": {}, "skill": {}, "stdin": {}, "wait": {}, "request-reply": {}, "raw": {}, "blockers": {}, "loaded": {}, "archived": {}, "portable": {}, "content": {}, "dry-run": {}, "fix": {}, "force": {},
	"endpoint": {value: true}, "config": {value: true}, "state-dir": {value: true}, "color": {value: true}, "file": {value: true}, "delivery-timeout": {value: true}, "wait-timeout": {value: true}, "timeout": {value: true}, "reply-to": {value: true}, "status": {value: true}, "error-code": {value: true}, "receipt-file": {value: true}, "receipts": {value: true}, "receipts-cursor": {value: true}, "cwd": {value: true}, "source": {value: true, repeat: true}, "sort": {value: true}, "order": {value: true}, "limit": {value: true}, "cursor": {value: true}, "view": {value: true}, "turn": {value: true}, "model": {value: true}, "name": {value: true}, "through-turn": {value: true}, "before-turn": {value: true}, "thread": {value: true}, "state": {value: true}, "since": {value: true}, "resolve-as": {value: true}, "reason": {value: true}, "evidence": {value: true}, "ssh": {value: true}, "unix": {value: true}, "id": {value: true}, "herdr": {value: true}, "before": {value: true}, "params": {value: true}, "params-file": {value: true}, "allow-effect": {value: true, repeat: true}, "output": {value: true},
}

// Parse validates syntax-independent option shape and returns the invocation.
// Command-specific constraints are checked by validateInvocation.
func Parse(args []string) (Invocation, error) {
	inv := Invocation{Options: make(map[string][]string)}
	var positional []string
	optionsEnded := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !optionsEnded && arg == "--" {
			optionsEnded = true
			continue
		}
		if !optionsEnded && (strings.HasPrefix(arg, "--") || arg == "-h" || arg == "-v") {
			name, value, hasValue, err := parseOption(arg)
			if err != nil {
				return inv, usageError(err.Error())
			}
			if name == "v" {
				return inv, usageError("use mektup version")
			}
			spec, ok := optionSpecs[name]
			if !ok {
				return inv, usageError("unknown option: --" + name)
			}
			if spec.value {
				if !hasValue {
					if i+1 >= len(args) || strings.HasPrefix(args[i+1], "-") {
						return inv, usageError("option --" + name + " requires a value")
					}
					i++
					value = args[i]
				}
			} else if hasValue {
				return inv, usageError("option --" + name + " does not take a value")
			}
			if name == "help" {
				inv.Global.Help = true
			}
			if name == "json" {
				inv.Global.JSON = true
			}
			if name == "human" {
				inv.Global.Human = true
			}
			if name == "compact" {
				inv.Global.Compact = true
			}
			if name == "debug" {
				inv.Global.Debug = true
			}
			if name == "audit" {
				inv.Global.Audit = true
			}
			if name == "endpoint" {
				inv.Global.Endpoint = value
			}
			if name == "config" {
				inv.Global.Config = value
			}
			if name == "state-dir" {
				inv.Global.StateDir = value
			}
			if name == "color" {
				inv.Global.Color = value
			}
			if !spec.repeat && len(inv.Options[name]) > 0 {
				return inv, usageError("option --" + name + " may only be supplied once")
			}
			if spec.value {
				inv.Options[name] = append(inv.Options[name], value)
			} else {
				inv.Options[name] = append(inv.Options[name], "true")
			}
			continue
		}
		positional = append(positional, arg)
	}
	if len(positional) > 0 {
		inv.Command = positional[0]
		inv.Position = positional[1:]
		inv.Path = []string{inv.Command}
		if len(inv.Position) > 0 {
			switch inv.Command {
			case "thread", "receipt", "endpoint", "storage":
				inv.Path = append(inv.Path, inv.Position[0])
			case "help":
				// help topics remain positional content, not a command path.
			}
		}
	}
	if len(positional) == 0 && len(inv.Options["skill"]) > 0 {
		inv.Command = "--skill"
		inv.Path = []string{"--skill"}
	}
	return inv, nil
}

func parseOption(arg string) (name, value string, hasValue bool, err error) {
	if arg == "-h" {
		return "help", "", false, nil
	}
	if arg == "-v" {
		return "v", "", false, nil
	}
	if !strings.HasPrefix(arg, "--") || len(arg) <= 2 {
		return "", "", false, fmt.Errorf("invalid option: %s", arg)
	}
	nameValue := strings.TrimPrefix(arg, "--")
	if strings.Contains(nameValue, "=") {
		name, value, _ = strings.Cut(nameValue, "=")
		return name, value, true, nil
	}
	return nameValue, "", false, nil
}

func validateInvocation(a *App, inv Invocation) *Error {
	if boolCount(inv.Global.JSON, inv.Global.Human, inv.Global.Compact) > 1 {
		return usageError("--json, --human, and --compact are mutually exclusive")
	}
	if len(inv.Path) == 0 {
		return nil
	}
	allowed := allowedOptions(strings.Join(inv.Path, " "))
	for option := range inv.Options {
		if option == "help" || option == "json" || option == "human" || option == "compact" || option == "debug" || option == "audit" || option == "endpoint" || option == "config" || option == "state-dir" || option == "color" {
			continue
		}
		if !allowed[option] {
			return usageError(fmt.Sprintf("option --%s is not valid for %s", option, strings.Join(inv.Path, " ")))
		}
	}
	if inv.Command == "help" {
		return nil
	}
	if inv.Command == "doctor" {
		if len(inv.Position) != 0 {
			return usageError("usage: mektup doctor [--fix]")
		}
		return nil
	}
	if inv.Command == "rpc" {
		if len(inv.Position) != 1 {
			return usageError("usage: mektup rpc <method>")
		}
		params := 0
		for _, name := range []string{"params", "params-file", "stdin"} {
			params += len(inv.Options[name])
		}
		if params > 1 {
			return usageError("rpc accepts exactly one params source")
		}
		if err := validateOptionSyntax(inv); err != nil {
			return err
		}
		return nil
	}

	switch inv.Command {
	case "send":
		if len(inv.Position) < 1 || len(inv.Position) > 2 {
			return usageError("usage: mektup send <target> [message|--stdin|--file <path>]")
		}
		if inv.Position[0] == "" {
			return &Error{Code: "invalid_target", Message: "target must not be empty", Exit: ExitUsage}
		}
		if err := validateBodySource(a, inv, 1); err != nil {
			return err
		}
		if has(inv, "raw") && has(inv, "wait") {
			return &Error{Code: "invalid_raw_wait", Message: "--raw cannot be combined with --wait", Exit: ExitUsage}
		}
		if has(inv, "raw") && has(inv, "request-reply") {
			return &Error{Code: "invalid_raw_reply_request", Message: "--raw cannot be combined with --request-reply", Exit: ExitUsage}
		}
		if has(inv, "wait-timeout") && !has(inv, "wait") {
			return usageError("--wait-timeout requires --wait")
		}
	case "reply":
		if len(inv.Position) < 1 || len(inv.Position) > 2 {
			return usageError("usage: mektup reply <message-or-receipt-id> [message|--stdin|--file <path>]")
		}
		if inv.Position[0] == "" {
			return usageError("message-or-receipt-id must not be empty")
		}
		if err := validateBodySource(a, inv, 1); err != nil {
			return err
		}
		if value := inv.Option("error-code"); value != "" && inv.Option("status") != "error" {
			return usageError("--error-code requires --status error")
		}
		if status := inv.Option("status"); status != "" && status != "success" && status != "error" {
			return usageError("--status must be success or error")
		}
		if has(inv, "wait-timeout") && !has(inv, "wait") {
			return usageError("--wait-timeout requires --wait")
		}
	case "wait":
		if len(inv.Position) != 1 {
			return usageError("usage: mektup wait <receipt-or-message-id>")
		}
		if inv.Position[0] == "" {
			return usageError("receipt-or-message-id must not be empty")
		}
	case "inspect":
		if len(inv.Position) != 1 {
			return usageError("usage: mektup inspect <target>")
		}
		if inv.Position[0] == "" {
			return &Error{Code: "invalid_target", Message: "target must not be empty", Exit: ExitUsage}
		}
	case "search":
		if len(inv.Position) != 1 {
			return usageError("usage: mektup search <query>")
		}
		if inv.Position[0] == "" {
			return usageError("query must not be empty")
		}
		if has(inv, "thread") && (has(inv, "archived") || has(inv, "source")) {
			return usageError("--archived and --source are invalid with --thread")
		}
	case "thread", "receipt", "endpoint", "storage":
		if err := validateNested(inv); err != nil {
			return err
		}
	default:
		return usageError("unknown command: " + inv.Command)
	}
	if err := validateOptionSyntax(inv); err != nil {
		return err
	}
	return nil
}

func validateGlobals(inv Invocation) *Error {
	for _, option := range []string{"endpoint", "config", "state-dir"} {
		if has(inv, option) && strings.TrimSpace(inv.Option(option)) == "" {
			return usageError("--" + option + " requires a non-empty value")
		}
	}
	return nil
}

func validateBodySource(a *App, inv Invocation, messageIndex int) *Error {
	sources := 0
	if len(inv.Position) > messageIndex {
		if inv.Position[messageIndex] == "" {
			return usageError("message body must not be empty")
		}
		sources++
	}
	if has(inv, "stdin") {
		sources++
	}
	if has(inv, "file") {
		sources++
	}
	if sources > 1 {
		return usageError("message body accepts exactly one source: positional, --stdin, or --file")
	}
	if sources == 0 && stdinIsInteractive(a.In) {
		return usageError("message body is required when stdin is interactive")
	}
	if has(inv, "file") && inv.Option("file") == "" {
		return usageError("--file requires a path")
	}
	return nil
}

func validateOptionSyntax(inv Invocation) *Error {
	for _, option := range []string{"delivery-timeout", "wait-timeout", "timeout"} {
		if has(inv, option) {
			value := inv.Option(option)
			parsed, err := time.ParseDuration(value)
			if err != nil || parsed < 0 {
				return usageError("--" + option + " requires a valid duration")
			}
		}
	}
	for _, option := range []string{"limit", "receipts"} {
		if has(inv, option) {
			value := inv.Option(option)
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return usageError("--" + option + " requires a non-negative integer")
			}
			if inv.Resolved.Compact && n > CompactMaxLimit {
				return usageError("--" + option + " exceeds compact maximum 25; use explicit --json for a larger complete page")
			}
		}
	}
	if has(inv, "status") && inv.Option("status") != "success" && inv.Option("status") != "error" {
		return usageError("--status must be success or error")
	}
	if has(inv, "sort") && inv.Option("sort") != "updated" && inv.Option("sort") != "created" && inv.Option("sort") != "recent" {
		return usageError("--sort must be updated, created, or recent")
	}
	if has(inv, "order") && inv.Option("order") != "asc" && inv.Option("order") != "desc" {
		return usageError("--order must be asc or desc")
	}
	if has(inv, "view") && inv.Option("view") != "summary" && inv.Option("view") != "full" {
		return usageError("--view must be summary or full")
	}
	if has(inv, "herdr") && inv.Option("herdr") != "auto" && inv.Option("herdr") != "disabled" {
		return usageError("--herdr must be auto or disabled")
	}
	if has(inv, "resolve-as") && inv.Option("resolve-as") != "accepted" && inv.Option("resolve-as") != "not-delivered" {
		return usageError("--resolve-as must be accepted or not-delivered")
	}
	if value := inv.Option("output"); value == "" && has(inv, "output") {
		return usageError("--output requires a path")
	}
	for _, option := range []string{"source", "allow-effect"} {
		for _, value := range inv.Options[option] {
			if strings.TrimSpace(value) == "" {
				return usageError("--" + option + " values must not be empty")
			}
		}
	}
	return nil
}

func stdinIsInteractive(in io.Reader) bool {
	f, ok := in.(*os.File)
	if !ok {
		return false
	}
	info, err := f.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

func has(inv Invocation, option string) bool { return len(inv.Options[option]) > 0 }

func boolCount(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func validateNested(inv Invocation) *Error {
	if len(inv.Position) < 1 {
		return usageError("missing subcommand for " + inv.Command)
	}
	key := strings.Join(inv.Path, " ")
	if _, known := allowedOptionTable[key]; !known {
		return usageError("unknown subcommand: " + strings.Join(inv.Position, " "))
	}
	allowed := allowedOptions(key)
	if len(inv.Position) < positionalMinimum(key) || len(inv.Position) > positionalMaximum(key) {
		return usageError("invalid positional arguments for " + key)
	}
	switch key {
	case "thread read", "thread turns", "thread items", "thread resume", "thread fork", "receipt show", "receipt reconcile", "receipt resolve", "endpoint show", "endpoint remove":
		if len(inv.Position) > 1 && inv.Position[1] == "" {
			return usageError("target or receipt identifier must not be empty")
		}
	}
	for option := range inv.Options {
		if option == "help" || option == "json" || option == "human" || option == "compact" || option == "debug" || option == "audit" || option == "endpoint" || option == "config" || option == "state-dir" || option == "color" {
			continue
		}
		if !allowed[option] {
			return usageError(fmt.Sprintf("option --%s is not valid for %s", option, key))
		}
	}
	if key == "endpoint add" {
		routes := 0
		if has(inv, "ssh") {
			routes++
		}
		if has(inv, "unix") {
			routes++
		}
		if routes != 1 {
			return usageError("endpoint add requires exactly one of --ssh or --unix")
		}
	}
	if key == "thread fork" && has(inv, "through-turn") && has(inv, "before-turn") {
		return usageError("--through-turn and --before-turn are mutually exclusive")
	}
	if key == "receipt show" && has(inv, "portable") && has(inv, "content") {
		return usageError("--portable and --content are mutually exclusive")
	}
	if key == "receipt resolve" {
		resolution := inv.Option("resolve-as")
		if resolution != "accepted" && resolution != "not-delivered" {
			return usageError("receipt resolve requires --resolve-as accepted|not-delivered")
		}
		if inv.Option("reason") == "" || inv.Option("evidence") == "" {
			return usageError("receipt resolve requires --reason and --evidence")
		}
	}
	if err := validateOptionSyntax(inv); err != nil {
		return err
	}
	return nil
}

func positionalMinimum(key string) int {
	switch key {
	case "thread list", "receipt list", "endpoint list", "storage status", "storage check", "storage maintain", "storage vacuum":
		return 1 // includes the subcommand in Position
	case "thread start", "endpoint check":
		return 1
	default:
		return 2
	}
}

func positionalMaximum(key string) int {
	switch key {
	case "thread list", "receipt list", "endpoint list", "storage status", "storage check", "storage maintain", "storage vacuum", "thread start":
		return 1
	case "endpoint check":
		return 2
	case "thread fork":
		return 2
	default:
		return 2
	}
}

func allowedOptions(key string) map[string]bool {
	result := map[string]bool{}
	for _, option := range strings.Fields(allowedOptionTable[key]) {
		result[option] = true
	}
	return result
}

var allowedOptionTable = map[string]string{
	"send":              "stdin file request-reply wait delivery-timeout wait-timeout reply-to raw",
	"reply":             "stdin file status error-code wait delivery-timeout wait-timeout receipt-file reply-to",
	"wait":              "timeout receipt-file",
	"inspect":           "receipts receipts-cursor blockers",
	"search":            "thread archived source limit cursor",
	"thread list":       "loaded archived cwd source sort order limit cursor",
	"thread read":       "",
	"thread turns":      "view order limit cursor",
	"thread items":      "turn order limit cursor",
	"thread start":      "cwd model name",
	"thread resume":     "",
	"thread fork":       "through-turn before-turn name",
	"receipt list":      "state since limit cursor",
	"receipt show":      "portable content",
	"receipt reconcile": "",
	"receipt resolve":   "resolve-as reason evidence",
	"endpoint list":     "",
	"endpoint show":     "",
	"endpoint add":      "ssh unix id herdr",
	"endpoint remove":   "",
	"endpoint check":    "",
	"storage status":    "",
	"storage check":     "",
	"storage maintain":  "before dry-run",
	"storage vacuum":    "",
	"doctor":            "fix",
	"rpc":               "params params-file stdin allow-effect output force",
}

// Keep this import check explicit: callers embedding App may provide a reader
// that is not a file, while the default CLI still needs the standard reader.
var _ io.Reader = os.Stdin
