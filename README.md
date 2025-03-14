# How to collect data
1. Capture data with Wireshark
2. Use `tls.handshake.extensions_server_name contains "signal.org"` as Wireshark filter to find the IP of the Signal servers
3. Filter based on those IP's, eg. `(ip.addr == SIGNAL_SERVER_IP1 || ip.addr == SIGNAL_SERVER_IP2) && !tcp.analysis.keep_alive && !tcp.analysis.keep_alive_ack`
4. Output the filtered data as csv, and call it data.csv, this is important.
5. In the same folder as data.csv, create a settings.json file, here the name is also important.
  eg. 
```{
  "target": "IP_TARGET",
  "actual": "IP_ACTUAL_RECEIVER_FOR_TARGET",
  "epoch": 5,
  "server": ["SIGNAL_SERVER_IP1", "SIGNAL_SERVER_IP2"]
}```
