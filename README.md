# @platformatic/watt-plugin-kafka-health

A plugin for monitoring `@platformatic/kafka` consumers lag in Watt applications.

## Overview

This plugin monitors consumers lag and sends health signals when consumers are lagging behind the acceptable threshold.

It integrates with Watt's health monitoring system to provide real-time alerts when Kafka consumers cannot keep up with message production.

## Features

- **Consumer Lag Monitoring**: Tracks lag for all Kafka consumers in your application
- **Configurable Thresholds**: Set global and per-topic maximum allowed lag values
- **Smart Health Signals**: Only sends alerts when lag is excessive AND there are available consumers to scale
- **Grace Period**: Configurable delay before monitoring begins to avoid false positives during startup
- **Auto-discovery**: Automatically monitors all Kafka consumers created in the application

## Installation

```bash
npm install @platformatic/watt-plugin-kafka-health
```

## Usage

### Basic Usage

```javascript
import { create } from '@platformatic/watt-plugin-kafka-health'

// Create the health plugin with basic configuration
const healthPlugin = create({
  maxAllowedLag: 1000, // Global max lag threshold
  interval: 60000, // Check interval in milliseconds
  gracePeriod: 5000, // Wait before starting monitoring
  topics: {
    'high-priority': 500, // Custom lag threshold for specific topics
    'low-priority': 2000
  }
})
```

### Configuration Options

| Option          | Type                   | Default | Description                                        |
| --------------- | ---------------------- | ------- | -------------------------------------------------- |
| `maxAllowedLag` | number                 | 1000    | Global maximum allowed consumer lag                |
| `interval`      | number                 | 60000   | Interval for lag monitoring checks in milliseconds |
| `gracePeriod`   | number                 | 1000    | Delay before monitoring starts in milliseconds     |
| `topics`        | Record<string, number> | {}      | Per-topic lag thresholds                           |

### Health Signals

When lag exceeds thresholds, the plugin emits health signals with the following structure:

```javascript
{
  type: 'kafka:topics:lag',
  value: 1500,  // Current lag value
  description: 'Member consumer-123 of consumers group my-group is lagging for topic my-topic and there are available consumers in the group.'
}
```

## Requirements

- Node.js >= 22.19.0
- `@platformatic/kafka` ^1.20.0 as peer dependency

## License

Apache-2.0 - See [LICENSE](LICENSE) for more information.

## Contributing

Contributions are welcome! Please check the [issues page](https://github.com/platformatic/watt-plugin-kafka-health/issues) for known issues and feature requests.

## Support

- [GitHub Issues](https://github.com/platformatic/watt-plugin-kafka-health/issues)
- [Platformatic Documentation](https://docs.platformatic.dev/)
