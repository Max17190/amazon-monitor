# TVSS Transport Benchmark

## Decision

Keep aiohttp HTTP/1.1 as the TVSS production transport. Do not add
`curl_cffi`, a transport selector, or a browser impersonation profile.

The bounded Railway bakeoff did not find a candidate that met the adoption
gate. Each curl candidate had worse median and tail latency than the initial
aiohttp control. The final control also detected substantial edge latency
drift during the run, and every candidate's bootstrap confidence interval
included a regression.

## Method

The benchmark ran on 2026-07-18 in one `us-east4-eqdc4a` Railway replica with
direct egress and exactly 20 unique ASINs. It used one active request at a time
and a global five-second request-start cadence.

One unmeasured compatibility and connection warmup request preceded each
block. The measured sequence was:

1. 30 aiohttp HTTP/1.1 control requests
2. 60 curl_cffi HTTP/1.1 requests without impersonation
3. 60 curl_cffi HTTP/2 requests without impersonation
4. 60 curl_cffi HTTP/2 requests with the pinned `chrome146` profile and
   default browser headers disabled
5. 30 final aiohttp HTTP/1.1 control requests

All variants used long-lived single-connection pools, the exact Fire TV TVSS
headers, raw-byte reads, and monotonic nanosecond measurements. The run made
240 measured requests and five warmups. It completed with zero 429 responses,
transport failures, response mismatches, or classification mismatches.

## Results

| Transport | Protocol | Requests | Warm p50 | Warm p95 | Result |
| --- | --- | ---: | ---: | ---: | --- |
| aiohttp initial control | HTTP/1.1 | 30 | 177.701 ms | 313.955 ms | Control |
| curl_cffi without impersonation | HTTP/1.1 | 60 | 190.145 ms | 398.227 ms | Rejected |
| curl_cffi without impersonation | HTTP/2 | 60 | 201.492 ms | 403.594 ms | Rejected |
| curl_cffi with `chrome146` | HTTP/2 | 60 | 257.602 ms | 382.441 ms | Rejected |
| aiohttp final control | HTTP/1.1 | 30 | 281.587 ms | 537.868 ms | Drift control |

The combined aiohttp control was 197.605 ms p50 and 358.060 ms p95. Candidate
median delta bootstrap confidence intervals were:

| Candidate | 95% median delta confidence interval |
| --- | ---: |
| curl_cffi HTTP/1.1 | -66.464 ms to 26.859 ms |
| curl_cffi HTTP/2 | -56.828 ms to 71.743 ms |
| curl_cffi HTTP/2 with `chrome146` | -28.319 ms to 82.387 ms |

No candidate demonstrated the required repeatable 3 ms median improvement.
Each candidate also exceeded the combined aiohttp p95 by more than the allowed
two percent.

Warm body reads, JSON decoding, and classification were each approximately
0.16 ms or less at p50. The observed latency was dominated by time to first
byte, so adding TLS or HTTP fingerprinting complexity did not improve this
workload.

Railway reported 0.044 vCPU maximum and 34.876 MB maximum memory. The existing
10,000-iteration local hot-path benchmark passed in the same container at
0.059 ms p95. The one-shot deployment stopped after emitting the
`TVSS_TRANSPORT_CANARY_ACCEPTED` sentinel with `winner` set to `aiohttp`.

## Adoption Gate

A candidate could replace aiohttp only if all of the following held:

1. Warm p50 improved by at least 3 ms.
2. Warm p95 regressed by no more than two percent.
3. The bootstrap 95% confidence interval excluded improvements smaller than
   3 ms.
4. The run produced zero 429 responses, transport failures, response
   mismatches, and classification mismatches.

The reliability gate passed for all variants. The latency and confidence gates
failed for every curl variant.
