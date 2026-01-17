package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/yogoosoft/elasticrelay/api/gateway/v1"
	"github.com/yogoosoft/elasticrelay/internal/config"
	mongodb_connector "github.com/yogoosoft/elasticrelay/internal/connectors/mongodb"
	mysql_connector "github.com/yogoosoft/elasticrelay/internal/connectors/mysql"
	postgresql_connector "github.com/yogoosoft/elasticrelay/internal/connectors/postgresql"
	"github.com/yogoosoft/elasticrelay/internal/logger"
	orchestrator "github.com/yogoosoft/elasticrelay/internal/orchestrator"
	es_sink "github.com/yogoosoft/elasticrelay/internal/sink/es"
	transform "github.com/yogoosoft/elasticrelay/internal/transform"
	"github.com/yogoosoft/elasticrelay/internal/version"
)

// DummySinkServer is a sink server that immediately fails all operations
// This is used when the real sink (like Elasticsearch) is unavailable during startup
// It allows the application to continue running and route failed events to DLQ
type DummySinkServer struct {
	pb.UnimplementedSinkServiceServer
}

// BulkWrite immediately fails all write operations, triggering DLQ
func (d *DummySinkServer) BulkWrite(stream pb.SinkService_BulkWriteServer) error {
	log.Printf("DummySinkServer: BulkWrite called - immediately failing to trigger DLQ")
	return fmt.Errorf("sink unavailable - triggering DLQ")
}

// DescribeIndex fails all describe operations
func (d *DummySinkServer) DescribeIndex(ctx context.Context, req *pb.DescribeIndexRequest) (*pb.DescribeIndexResponse, error) {
	return nil, fmt.Errorf("sink unavailable")
}

func main() {
	// Command-line flags
	var (
		showVersion     = flag.Bool("version", false, "Show version information and exit")
		port            = flag.String("port", "50051", "gRPC service port")
		configFile      = flag.String("config", "config.json", "Path to the configuration file")
		transformConfig = flag.String("transform-config", "", "Path to the transform configuration file (optional)")
	)
	flag.Parse()

	// Handle version flag
	if *showVersion {
		version.DisplayLogo()
		os.Exit(0)
	}

	// Display startup logo and version information
	version.DisplayLogo()
	fmt.Println("🚀 ElasticRelay starting...")

	// Detect configuration format and load accordingly
	migrationService := config.NewMigrationService(*configFile)
	configVersion, err := migrationService.DetectConfigFormat()
	if err != nil {
		log.Fatalf("failed to detect config format: %v", err)
	}

	log.Printf("Detected configuration format version: %s", configVersion)

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Handle different configuration formats
	if configVersion == "2.0" || configVersion == "3.0" {
		// Use multi-config format
		log.Printf("Loading multi-configuration from %s", *configFile)
		multiCfg, err := config.LoadMultiConfig(*configFile)
		if err != nil {
			log.Fatalf("failed to load multi-configuration: %v", err)
		}

		// Create multi-orchestrator for handling multiple sources/sinks
		log.Println("Creating multi-orchestrator for handling multiple configurations...")
		orchServer, err := orchestrator.NewMultiOrchestrator("localhost:" + *port)
		if err != nil {
			log.Fatalf("failed to create multi orchestrator server: %v", err)
		}

		// Set global log level from configuration
		if multiCfg.Global.LogLevel != "" {
			logger.SetLogLevel(multiCfg.Global.LogLevel)
			log.Printf("Set log level to: %s", multiCfg.Global.LogLevel)
		}

		// Load multi-configuration
		if err := orchServer.LoadConfiguration(multiCfg); err != nil {
			log.Fatalf("failed to load multi-configuration into orchestrator: %v", err)
		}

		// Create and start jobs from configuration
		if err := orchServer.CreateJobsFromConfig(); err != nil {
			log.Fatalf("failed to create jobs from configuration: %v", err)
		}

		// Log connector types that were created
		for _, ds := range multiCfg.DataSources {
			log.Printf("%s Connector Server created for data source '%s'",
				strings.Title(ds.Type), ds.ID)
		}

		// Create transform server with optional configuration
		var transServer *transform.Server
		if *transformConfig != "" {
			log.Printf("Loading transform configuration from %s", *transformConfig)
			transformConfigs, err := transform.LoadTransformConfig(*transformConfig)
			if err != nil {
				log.Printf("⚠️  Warning: failed to load transform config: %v", err)
				log.Printf("⚠️  Transform engine will run in pass-through mode")
				transServer, err = transform.NewServer()
				if err != nil {
					log.Fatalf("failed to create transform server: %v", err)
				}
			} else {
				transServer, err = transform.NewServer(transform.WithConfigs(transformConfigs))
				if err != nil {
					log.Fatalf("failed to create transform server: %v", err)
				}
				log.Printf("✅ Transform engine loaded with %d rules", len(transformConfigs))
			}
		} else {
			transServer, err = transform.NewServer()
			if err != nil {
				log.Fatalf("failed to create transform server: %v", err)
			}
		}

		// Create sink servers for each sink configuration
		var sinkServers []pb.SinkServiceServer
		for _, sinkCfg := range multiCfg.Sinks {
			if sinkCfg.Type == "elasticsearch" {
				var sinkServer pb.SinkServiceServer
				realSinkServer, err := es_sink.NewServerFromSinkConfig(&sinkCfg)
				if err != nil {
					log.Printf("⚠️  Warning: failed to create es sink server for '%s': %v", sinkCfg.ID, err)
					log.Printf("⚠️  Creating dummy sink server - events will go to DLQ when sink fails")
					// Create a dummy sink server that will trigger DLQ for all operations
					sinkServer = &DummySinkServer{}
				} else {
					sinkServer = realSinkServer
				}
				sinkServers = append(sinkServers, sinkServer)
				log.Printf("Sink Server created for '%s' (may be in DLQ-only mode)", sinkCfg.ID)
			}
		}

		// Create connector server based on data source type for multi-config mode
		// Note: For legacy compatibility, we create a connector server from the first data source
		var connectorServer pb.ConnectorServiceServer
		if len(multiCfg.DataSources) > 0 {
			firstDS := multiCfg.DataSources[0]
			switch firstDS.Type {
			case "mysql":
				connectorServer, err = mysql_connector.NewServer(&config.Config{
					DBHost:       firstDS.Host,
					DBPort:       firstDS.Port,
					DBUser:       firstDS.User,
					DBPassword:   firstDS.Password,
					DBName:       firstDS.Database,
					ServerID:     firstDS.ServerID,
					TableFilters: firstDS.TableFilters,
				})
				if err != nil {
					log.Fatalf("failed to create mysql connector server for multi-config: %v", err)
				}
			case "postgresql":
				connectorServer, err = postgresql_connector.NewServer(&config.Config{
					DBHost:       firstDS.Host,
					DBPort:       firstDS.Port,
					DBUser:       firstDS.User,
					DBPassword:   firstDS.Password,
					DBName:       firstDS.Database,
					TableFilters: firstDS.TableFilters,
				})
				if err != nil {
					log.Fatalf("failed to create postgresql connector server for multi-config: %v", err)
				}
			case "mongodb":
				connectorServer, err = mongodb_connector.NewServer(&config.Config{
					DBHost:       firstDS.Host,
					DBPort:       firstDS.Port,
					DBUser:       firstDS.User,
					DBPassword:   firstDS.Password,
					DBName:       firstDS.Database,
					TableFilters: firstDS.TableFilters,
				})
				if err != nil {
					log.Fatalf("failed to create mongodb connector server for multi-config: %v", err)
				}
			default:
				log.Printf("Warning: unsupported data source type '%s', connector server not created", firstDS.Type)
			}
		}

		// Create and register services on gRPC server
		s := grpc.NewServer()
		pb.RegisterOrchestratorServiceServer(s, orchServer)
		pb.RegisterTransformServiceServer(s, transServer)

		// Register connector server if it was created
		if connectorServer != nil {
			pb.RegisterConnectorServiceServer(s, connectorServer)
		}

		// Register all sink servers
		if len(sinkServers) > 0 {
			// For now, register the first ES sink server as the main sink service
			// In the future, we might need a multiplexer for multiple sinks
			pb.RegisterSinkServiceServer(s, sinkServers[0])
		}

		log.Printf("gRPC server listening on port %s with multi-configuration support", *port)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
		return
	} else {
		// Use legacy single-config format
		log.Printf("Loading legacy configuration from %s", *configFile)
		cfg, err := config.LoadConfig(*configFile)
		if err != nil {
			log.Fatalf("failed to load configuration: %v", err)
		}
		log.Printf("Legacy configuration loaded from %s", *configFile)

		// Create service instances using legacy format
		connServer, err := mysql_connector.NewServer(cfg)
		if err != nil {
			log.Fatalf("failed to create mysql connector server: %v", err)
		}

		sinkServer, err := es_sink.NewServer(cfg)
		if err != nil {
			log.Fatalf("failed to create es sink server: %v", err)
		}

		orchServer, err := orchestrator.NewServer("localhost:" + *port)
		if err != nil {
			log.Fatalf("failed to create orchestrator server: %v", err)
		}

		// Create transform server with optional configuration (legacy mode)
		var transServer *transform.Server
		if *transformConfig != "" {
			log.Printf("Loading transform configuration from %s", *transformConfig)
			transformConfigs, err := transform.LoadTransformConfig(*transformConfig)
			if err != nil {
				log.Printf("⚠️  Warning: failed to load transform config: %v", err)
				log.Printf("⚠️  Transform engine will run in pass-through mode")
				transServer, err = transform.NewServer()
				if err != nil {
					log.Fatalf("failed to create transform server: %v", err)
				}
			} else {
				transServer, err = transform.NewServer(transform.WithConfigs(transformConfigs))
				if err != nil {
					log.Fatalf("failed to create transform server: %v", err)
				}
				log.Printf("✅ Transform engine loaded with %d rules", len(transformConfigs))
			}
		} else {
			transServer, err = transform.NewServer()
			if err != nil {
				log.Fatalf("failed to create transform server: %v", err)
			}
		}

		// Create and register all services on one gRPC server
		s := grpc.NewServer()
		pb.RegisterConnectorServiceServer(s, connServer)
		pb.RegisterSinkServiceServer(s, sinkServer)
		pb.RegisterOrchestratorServiceServer(s, orchServer)
		pb.RegisterTransformServiceServer(s, transServer)

		log.Printf("gRPC server listening on port %s with legacy configuration", *port)

		// Auto-start a default CDC job for legacy mode
		go func() {
			// Wait for server to be ready
			time.Sleep(2 * time.Second)

			log.Printf("Auto-starting default CDC job for legacy configuration...")

			// Create a default job for the configured table
			req := &pb.CreateJobRequest{
				Name:   "default-legacy-job",
				Config: "test_table", // Use first table from config or default
			}

			if len(cfg.TableFilters) > 0 {
				req.Config = cfg.TableFilters[0]
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Connect to local orchestrator
			conn, err := grpc.Dial("localhost:"+*port, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Failed to connect to orchestrator for auto-start: %v", err)
				return
			}
			defer conn.Close()

			client := pb.NewOrchestratorServiceClient(conn)
			job, err := client.CreateJob(ctx, req)
			if err != nil {
				log.Printf("Failed to auto-start CDC job: %v", err)
				return
			}

			log.Printf("✅ Auto-started CDC job: %s (ID: %s)", job.Name, job.Id)
		}()

		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
		return
	}
}
