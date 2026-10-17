package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/rachmanzz/anchordb/pkg/driver"
)

const (
	cliVersion = "v0.1.1"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// extractGlobalFlags parses global flags (-url, -token, -model, -timeout) from args
// regardless of whether they appear before or after subcommand arguments, returning
// the configuration and remaining non-global command tokens.
func extractGlobalFlags(args []string, defaultURL, defaultToken, defaultModel string, defaultTimeout time.Duration) (url, token, model string, timeout time.Duration, remaining []string, err error) {
	url = defaultURL
	token = defaultToken
	model = defaultModel
	timeout = defaultTimeout

	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "-url" || arg == "--url":
			if i+1 >= len(args) {
				return "", "", "", 0, nil, errors.New("flag needs an argument: " + arg)
			}
			url = args[i+1]
			i++
		case strings.HasPrefix(arg, "-url=") || strings.HasPrefix(arg, "--url="):
			parts := strings.SplitN(arg, "=", 2)
			url = parts[1]

		case arg == "-token" || arg == "--token":
			if i+1 >= len(args) {
				return "", "", "", 0, nil, errors.New("flag needs an argument: " + arg)
			}
			token = args[i+1]
			i++
		case strings.HasPrefix(arg, "-token=") || strings.HasPrefix(arg, "--token="):
			parts := strings.SplitN(arg, "=", 2)
			token = parts[1]

		case arg == "-model" || arg == "--model":
			if i+1 >= len(args) {
				return "", "", "", 0, nil, errors.New("flag needs an argument: " + arg)
			}
			model = args[i+1]
			i++
		case strings.HasPrefix(arg, "-model=") || strings.HasPrefix(arg, "--model="):
			parts := strings.SplitN(arg, "=", 2)
			model = parts[1]

		case arg == "-timeout" || arg == "--timeout":
			if i+1 >= len(args) {
				return "", "", "", 0, nil, errors.New("flag needs an argument: " + arg)
			}
			d, parseErr := time.ParseDuration(args[i+1])
			if parseErr != nil {
				return "", "", "", 0, nil, fmt.Errorf("invalid timeout value: %w", parseErr)
			}
			timeout = d
			i++
		case strings.HasPrefix(arg, "-timeout=") || strings.HasPrefix(arg, "--timeout="):
			parts := strings.SplitN(arg, "=", 2)
			d, parseErr := time.ParseDuration(parts[1])
			if parseErr != nil {
				return "", "", "", 0, nil, fmt.Errorf("invalid timeout value: %w", parseErr)
			}
			timeout = d

		default:
			remaining = append(remaining, arg)
		}
	}
	return url, token, model, timeout, remaining, nil
}

func run(args []string) error {
	defaultURL := os.Getenv("ANCHORDB_URL")
	if defaultURL == "" {
		defaultURL = "http://localhost:7447"
	}
	defaultToken := os.Getenv("ANCHORDB_TOKEN")
	defaultModel := os.Getenv("ANCHORDB_MODEL")
	if defaultModel == "" {
		defaultModel = "default"
	}

	urlFlag, tokenFlag, modelFlag, timeoutFlag, remaining, err := extractGlobalFlags(args, defaultURL, defaultToken, defaultModel, 10*time.Second)
	if err != nil {
		return err
	}

	if len(remaining) < 1 {
		printUsage()
		return nil
	}

	cmd := remaining[0]
	subArgs := remaining[1:]

	// Handle global help / version before dispatching
	switch cmd {
	case "help", "-h", "--help":
		printUsage()
		return nil
	case "version", "-v", "--version":
		fmt.Printf("AnchorDB CLI %s (%s/%s, runtime: %s)\n", cliVersion, runtime.GOOS, runtime.GOARCH, runtime.Version())
		return nil
	}

	client, err := driver.ConnectRemote(urlFlag,
		driver.WithToken(tokenFlag),
		driver.WithModel(modelFlag),
		driver.WithTimeout(timeoutFlag),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize client: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeoutFlag)
	defer cancel()

	switch cmd {
	case "ping":
		return cmdPing(ctx, urlFlag, tokenFlag)
	case "status":
		return cmdStatus(ctx, urlFlag, tokenFlag)
	case "get":
		return cmdGet(ctx, client, subArgs)
	case "save":
		return cmdSave(ctx, client, subArgs)
	case "list":
		return cmdList(ctx, client, subArgs)
	case "schema":
		return cmdSchema(ctx, client, subArgs)
	case "causal":
		return cmdCausal(ctx, client, subArgs)
	case "chain":
		return cmdChain(ctx, client, subArgs)
	case "sync":
		return cmdSync(ctx, client)
	default:
		printUsage()
		return fmt.Errorf("unknown command: %q", cmd)
	}
}

func printUsage() {
	fmt.Println(`AnchorDB Official Command-Line Interface (CLI)

Usage:
  anchordb <command> [arguments] [flags]

Core Commands:
  ping                          Test connection to the AnchorDB server
  status                        Display server metrics, memory, and scheduler status
  get <name>                    Retrieve an entity by name
  save <name> <k=v ...>         Store an entity with key-value numerical properties
  list                          List entities with optional filters and pagination
  schema get                    Display the locked schema contract for the active model
  schema attach <field:type...> Attach and permanently lock a schema contract
  causal <seed> <rel>           Follow 1-hop causal edges from a seed node
  chain <seed> <rel1,rel2...>   Follow multi-hop causal reasoning chain
  sync                          Flush storage engine buffers and WAL to disk
  version                       Display CLI version and build information
  help                          Display this help text

Global Flags (usable before or after subcommands):
  -url string       AnchorDB server URL (default: http://localhost:7447, env: ANCHORDB_URL)
  -token string     Bearer authentication token (env: ANCHORDB_TOKEN)
  -model string     Target model database name (default: default, env: ANCHORDB_MODEL)
  -timeout duration Request timeout (default: 10s)

Examples:
  anchordb ping
  anchordb save sensor_1 distance=120 duration=2.0 -model plant_iot
  anchordb get sensor_1 -model plant_iot
  anchordb list -page 2 -limit 10 -model plant_iot
  anchordb schema attach voltage:float current:float status:int -model plant_iot
  anchordb schema get -model plant_iot
  anchordb causal fuel_shortage causes -model events`)
}

func cmdPing(ctx context.Context, baseURL, token string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/health", nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	t0 := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	defer resp.Body.Close()
	lat := time.Since(t0)

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("PONG: %s is healthy (latency: %v)\n", baseURL, lat)
		return nil
	}
	return fmt.Errorf("%s returned HTTP status %d (latency: %v)", baseURL, resp.StatusCode, lat)
}

func cmdStatus(ctx context.Context, baseURL, token string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/health", nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("querying server status: %w", err)
	}
	defer resp.Body.Close()

	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return fmt.Errorf("parsing server status response: %w", err)
	}

	out, _ := json.MarshalIndent(data, "", "  ")
	fmt.Println(string(out))
	return nil
}

func cmdGet(ctx context.Context, client *driver.Client, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: anchordb get <entity_name>")
	}
	name := args[0]
	rec, err := client.Get(name).Execute(ctx)
	if err != nil {
		return fmt.Errorf("get failed: %w", err)
	}

	output := map[string]any{
		"name":       rec.Name,
		"model":      client.ModelName(),
		"properties": rec.Props(),
	}
	out, _ := json.MarshalIndent(output, "", "  ")
	fmt.Println(string(out))
	return nil
}

func cmdSave(ctx context.Context, client *driver.Client, args []string) error {
	if len(args) < 2 {
		return errors.New("usage: anchordb save <entity_name> <field1=value1> [field2=value2 ...]")
	}
	name := args[0]
	props := make(map[string]float32)

	for _, pair := range args[1:] {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid property argument: %q (expected key=value)", pair)
		}
		val, err := strconv.ParseFloat(parts[1], 32)
		if err != nil {
			return fmt.Errorf("invalid float32 value for %q: %w", parts[0], err)
		}
		props[parts[0]] = float32(val)
	}

	if err := client.Save(name, props); err != nil {
		return fmt.Errorf("save failed: %w", err)
	}

	fmt.Printf("OK: saved entity %q to model %q with %d properties\n", name, client.ModelName(), len(props))
	return nil
}

func cmdList(ctx context.Context, client *driver.Client, args []string) error {
	listFlags := flag.NewFlagSet("list", flag.ContinueOnError)
	page := listFlags.Int("page", 1, "Page number")
	limit := listFlags.Int("limit", 20, "Page size limit")
	where := listFlags.String("where", "", "Filter condition: 'field op value' (e.g. 'voltage >= 10')")

	if err := listFlags.Parse(args); err != nil {
		return err
	}

	builder := client.List().Page(*page, *limit)
	if *where != "" {
		parts := strings.Fields(*where)
		if len(parts) != 3 {
			return fmt.Errorf("invalid where filter: %q (expected 'field op value')", *where)
		}
		val, err := strconv.ParseFloat(parts[2], 32)
		if err != nil {
			return fmt.Errorf("invalid filter numeric value %q: %w", parts[2], err)
		}
		builder.Where(parts[0], parts[1], float32(val))
	}

	res, err := builder.Execute(ctx)
	if err != nil {
		return fmt.Errorf("list failed: %w", err)
	}

	fmt.Printf("Model: %s | Page %d | Items: %d | HasMore: %v\n\n", client.ModelName(), *page, len(res.Items), res.HasMore)
	if len(res.Items) == 0 {
		fmt.Println("No records found.")
		return nil
	}

	for _, item := range res.Items {
		propsJSON, _ := json.Marshal(item.Props())
		fmt.Printf("• %-24s -> %s\n", item.Name, string(propsJSON))
	}
	return nil
}

func cmdSchema(ctx context.Context, client *driver.Client, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: anchordb schema [get|attach <field:type ...>]")
	}
	action := args[0]

	switch action {
	case "get":
		schema, err := client.GetSchema(ctx)
		if err != nil {
			return fmt.Errorf("get schema failed: %w", err)
		}
		out, _ := json.MarshalIndent(schema, "", "  ")
		fmt.Printf("Model: %s\nLocked Schema Contract:\n%s\n", client.ModelName(), string(out))
		return nil

	case "attach":
		if len(args) < 2 {
			return errors.New("usage: anchordb schema attach <field1:type1> [field2:type2 ...]")
		}
		schema := make(driver.Schema)
		for _, pair := range args[1:] {
			parts := strings.SplitN(pair, ":", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid schema argument: %q (expected field:type)", pair)
			}
			schema[parts[0]] = parts[1]
		}
		if err := client.Attach(schema); err != nil {
			return fmt.Errorf("attach schema failed: %w", err)
		}
		fmt.Printf("OK: schema contract locked successfully to model %q with %d fields\n", client.ModelName(), len(schema))
		return nil

	default:
		return fmt.Errorf("unknown schema action: %q (expected 'get' or 'attach')", action)
	}
}

func parseTraversalArgs(args []string) (positional []string, k int, err error) {
	k = 5
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-k" || arg == "--k" {
			if i+1 >= len(args) {
				return nil, 0, fmt.Errorf("flag needs an argument: %s", arg)
			}
			i++
			arg = args[i]
		} else if strings.HasPrefix(arg, "-k=") || strings.HasPrefix(arg, "--k=") {
			_, arg, _ = strings.Cut(arg, "=")
		} else if strings.HasPrefix(arg, "-") {
			return nil, 0, fmt.Errorf("unknown traversal flag: %s", arg)
		} else {
			positional = append(positional, arg)
			continue
		}
		value, parseErr := strconv.Atoi(arg)
		if parseErr != nil || value <= 0 {
			return nil, 0, fmt.Errorf("invalid -k value %q: expected a positive integer", arg)
		}
		k = value
	}
	if len(positional) != 2 {
		return nil, 0, errors.New("expected a seed and a relation")
	}
	return positional, k, nil
}

func cmdCausal(ctx context.Context, client *driver.Client, args []string) error {
	positional, k, err := parseTraversalArgs(args)
	if err != nil {
		return fmt.Errorf("usage: anchordb causal <seed> <relation> [-k 5]: %w", err)
	}
	seed := positional[0]
	rel := positional[1]

	results, err := client.Causal(seed, rel, k)
	if err != nil {
		return fmt.Errorf("causal query failed: %w", err)
	}

	fmt.Printf("Causal 1-Hop Query: seed=%q relation=%q (Top %d):\n", seed, rel, len(results))
	for i, r := range results {
		fmt.Printf("   [%d] Target: %-24s | Score: %.4f\n", i+1, r.Word, r.Score)
	}
	return nil
}

func cmdChain(ctx context.Context, client *driver.Client, args []string) error {
	positional, k, err := parseTraversalArgs(args)
	if err != nil {
		return fmt.Errorf("usage: anchordb chain <seed> <rel1,rel2,...> [-k 5]: %w", err)
	}
	seed := positional[0]
	relations := strings.Split(positional[1], ",")

	results, err := client.CausalChain([]string{seed}, relations, k)
	if err != nil {
		return fmt.Errorf("causal chain query failed: %w", err)
	}

	fmt.Printf("Causal Multi-Hop Chain: seed=%q relations=%v (Top %d):\n", seed, relations, len(results))
	for i, r := range results {
		fmt.Printf("   [%d] Target: %-24s | Score: %.4f\n", i+1, r.Word, r.Score)
	}
	return nil
}

func cmdSync(ctx context.Context, client *driver.Client) error {
	if err := client.Sync(); err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}
	fmt.Printf("OK: storage engine buffers flushed to disk for model %q\n", client.ModelName())
	return nil
}
