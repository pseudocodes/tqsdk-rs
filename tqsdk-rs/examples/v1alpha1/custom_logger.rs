//! 自定义日志 Layer 组合示例
//!
//! 演示如何将 tqsdk-rs 的日志 Layer 与业务层的其他 Layer 组合使用

use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, fmt};
use tqsdk_rs::create_logger_layer;

fn main() {
    println!("=== 自定义日志 Layer 组合示例 ===\n");

    // 示例 1: 使用 tqsdk-rs 提供的便捷方法
    println!("【示例 1】使用便捷方法初始化");
    tqsdk_rs::init_logger("debug", false);
    info!("使用便捷方法初始化的日志");
    println!();

    // 示例 2: 组合多个 Layer
    println!("【示例 2】组合多个 Layer");
    
    // 创建 tqsdk-rs 的 Layer
    let tqsdk_layer = create_logger_layer("debug", false);
    
    // 创建业务层的 Layer（例如：输出到文件）
    let file_appender = tracing_appender::rolling::daily("./logs", "app.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    let file_layer = fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)  // 文件不需要颜色
        .with_target(true)
        .with_line_number(true);
    
    // 组合多个 Layer
    tracing_subscriber::registry()
        .with(tqsdk_layer)
        .with(file_layer)
        .init();
    
    info!("这条日志会同时输出到控制台和文件");
    warn!("警告信息也会被记录");
    
    println!("\n【示例 3】高级用法：不同模块使用不同的日志级别");
    println!("提示：可以使用 EnvFilter 为不同模块设置不同的日志级别");
    println!("示例：EnvFilter::new(\"tqsdk_rs=debug,my_app=info,hyper=warn\")");
    
    println!("\n✓ 日志系统配置完成");
    println!("\n提示：");
    println!("1. 使用 create_logger_layer() 获取 Layer 进行组合");
    println!("2. 使用 init_logger() 快速初始化（适合简单场景）");
    println!("3. 日志文件已保存到 ./logs/app.log");
}
