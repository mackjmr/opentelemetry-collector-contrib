# Ack Extension
<!-- status autogenerated section -->
| Status        |           |
| ------------- |-----------|
| Stability     | [development]  |
| Distributions | [contrib], [splunk] |
| Issues        | [![Open issues](https://img.shields.io/github/issues-search/open-telemetry/opentelemetry-collector-contrib?query=is%3Aissue%20is%3Aopen%20label%3Aextension%2Fack%20&label=open&color=orange&logo=opentelemetry)](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues?q=is%3Aopen+is%3Aissue+label%3Aextension%2Fack) [![Closed issues](https://img.shields.io/github/issues-search/open-telemetry/opentelemetry-collector-contrib?query=is%3Aissue%20is%3Aclosed%20label%3Aextension%2Fack%20&label=closed&color=blue&logo=opentelemetry)](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues?q=is%3Aclosed+is%3Aissue+label%3Aextension%2Fack) |
| [Code Owners](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/CONTRIBUTING.md#becoming-a-code-owner)    | [@zpzhuSplunk](https://www.github.com/zpzhuSplunk), [@splunkericl](https://www.github.com/splunkericl) |

[development]: https://github.com/open-telemetry/opentelemetry-collector#development
[contrib]: https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol-contrib
[splunk]: https://github.com/signalfx/splunk-otel-collector
<!-- end autogenerated section -->

This extension allows acking of data upon successful processing. The upstream agent can choose to send event again
if ack fails. 
## Configuration

```yaml
extensions:
  ack:
    storage: 

receivers:
  splunk_hec:
    ack_extension: ack


service:
  extensions: [ack]
  pipelines:
    logs:
      receivers: [splunk_hec]
```