package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	json "github.com/bytedance/sonic"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	logger     *zap.Logger
	configFile = flag.String("config", "config.toml", "配置文件")

	labelNames   = []string{"env", "bucketName", "region", "type"}
	bucketGaugue = NewGauge("aliyun_oss_bucket_info", "aliyun oss bucket info", labelNames)

	metricList  = []string{
		"MeteringStorageUtilization",	// 存储大小
		"MeteringInternetTX",		// 公网流出计量流量
		"MeteringCdnTX",		// cdn流出计量流量
		"MeteringSyncTX",		// 跨区域复制流出计量流量
	}
)

func init() {
	logger, _ = zap.NewProduction()
}

type Config struct {
	ListenAddress         string                  `toml:"listen-address"`
	RefreshIntervalSecond int                     `toml:"refresh-interval-second"`
	Envs                  map[string]AliOssClient `toml:"envs"`
}

type AliOssClient struct {
	Region          string `toml:"region"`
	AccessKeyId     string `toml:"accessKeyId"`
	AccessKeySecret string `toml:"accessKeySecret"`
}

type AliOssBucketResp struct {
	Timestamp   int64   `json:"timestamp"`
	UserId      string  `json:"userId"`
	BucketName  string  `json:"BucketName"`
	StorageType string  `json:"storageType,omitempty"`
	Region      string  `json:"region,omitempty"`
	Value       float64 `json:"value"`
}

func newCmsClient(region, accessKeyId, accessKeySecret string) *cms.Client {
	config := sdk.NewConfig()
	credential := credentials.NewAccessKeyCredential(accessKeyId, accessKeySecret)
	cmsClient, err := cms.NewClientWithOptions(region, config, credential)
	if err != nil {
		panic(err)
	}

	return cmsClient
}

func getOssBucketMetrics(ctx context.Context, client *cms.Client, env string, metricName, metricType string) error {
	bucketResp := make([]AliOssBucketResp, 0)

	req := cms.CreateDescribeMetricLastRequest()
	req.Scheme = "https"
	req.ConnectTimeout = time.Duration(30) * time.Second
	req.ReadTimeout = time.Duration(120) * time.Second
	req.Namespace = "acs_oss_dashboard"
	req.MetricName = metricName

	resp, err := client.DescribeMetricLast(req)
	if err != nil {
		logger.Error("Encounter response error from Aliyun:", zap.Error(err))
		return err
	} else if err := json.Unmarshal([]byte(resp.Datapoints), &bucketResp); err != nil {
		logger.Error("Cannot decode json response: ", zap.Error(err))
		return err
	}

	for _, val := range bucketResp {
		bucketGaugue.WithLabelValues(env, val.BucketName, val.Region, metricType).Set(val.Value)
	}
	return nil
}

func runEnvTask(ctx context.Context, env string, cli AliOssClient, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	client := newCmsClient(cli.Region, cli.AccessKeyId, cli.AccessKeySecret)

	// 程序启动就先执行一次，然后再 Ticker 周期性运行
	// 参考文档：https://cloudmonitornext.console.aliyun.com/resources-list/metric/acs_oss_dashboard/oss/all
	for _, mn := range metricList {
		go func(metricName string) {
			getOssBucketMetrics(ctx, client, env, metricName, metricName)
		}(mn)
	}

	for {
		select {
		case <-ticker.C:
			for _, mn := range metricList {
				go func(metricName string) {
					getOssBucketMetrics(ctx, client, env, metricName, metricName)
				}(mn)
			}
		case <-ctx.Done():
			logger.Info("getOssBucketMetrics ctx done")
			return
		}
	}
}

func NewGauge(name, help string, labels []string) *prometheus.GaugeVec {
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "",
			Name:      name,
			Help:      help,
		}, labels)
	prometheus.MustRegister(gauge)
	return gauge
}

func main() {
	flag.Parse()
	defer logger.Sync()

	var config Config
	_, err := toml.DecodeFile(*configFile, &config)
	if err != nil {
		logger.Error("无法Decode toml 配置文件", zap.Error(err))
		os.Exit(1)
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	http.Handle("/metrics", promhttp.Handler())
	httpServer := http.Server{
		Addr: config.ListenAddress,
	}

	// 等待程序退出
	go func(ctx context.Context) {
		<-signalCh
		cancel()
		httpServer.Shutdown(ctx)
	}(ctx)

	for env, AliOssCli := range config.Envs {
		dur := time.Duration(config.RefreshIntervalSecond) * time.Second
		go runEnvTask(ctx, env, AliOssCli, dur)
	}

	logger.Info("http server started")
	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		logger.Fatal("HTTP server ListenAndServe Error: %v", zap.Error(err))
	}
}
