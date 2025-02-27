import re


def get_send_recv(log_data):
    send_pattern = r'// (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z) .*? Sending queued (regular|deniable) message: From: \{"name":"(\d+)","deviceId":1\}, To: \{"name":"(\d+)","deviceId":1\}'
    receive_pattern = r'// (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z) .*? Message received: From: \{"name":"(\d+)","deviceId":1\}, To: \{"name":"(\d+)","deviceId":1\}, Deniable: (true|false)'

    # // 2025-02-25T12:39:20.300Z [ExperimentClient] [32minfo[39m: Message received: From: {"name":"2","deviceId":1}, To: {"name":"0","deviceId":1}, Deniable: false, Type: TEXT
    # _send_pattern = "// (.*) [ExperimentClient] .*   Message received: From: (.*), To: (.*), Deniable: (.*?)"

    sent_matches = re.findall(send_pattern, log_data)
    received_matches = re.findall(receive_pattern, log_data)

    sent_messages = [(t, s, r, "r" if msg_type == "regular" else "d") for t, msg_type, s, r in sent_matches]
    received_messages = [(t, s, r, "r" if deniable == "false" else "d") for t, s, r, deniable in received_matches]

    return sent_messages, received_messages
