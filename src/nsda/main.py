import pandas as pd

# from pandas import DataFrame
# import numpy as np
from helper.denim_out_parser import get_send_recv


def main():
    with open("data/denim-1st-capture/output/client-log/0") as file:
        sent, recv = get_send_recv(file.read())
        print("Sent ", sent)
        print("Recv ", recv)

    return
    # Simulated message log: (Sender, Receiver)
    message_log = [
        ("Alice", "Bob"),
        ("Alice", "Bob"),
        ("Bob", "Alice"),
        ("Bob", "Charlie"),
        ("Charlie", "Alice"),
        ("Charlie", "Bob"),
        ("Bob", "Charlie"),
        ("Alice", "Bob"),
        ("Charlie", "Alice"),
        ("Charlie", "Bob"),
    ]

    # Convert log to DataFrame
    df = pd.DataFrame(message_log, columns=["Sender", "Receiver"])

    # Compute sender probability P(S_i)
    sender_counts = df["Sender"].value_counts(normalize=True)

    # Compute receiver probability P(R_j)
    receiver_counts = df["Receiver"].value_counts(normalize=True)

    # Compute joint probability P(M_{i,j})
    joint_counts = df.groupby(["Sender", "Receiver"]).size()
    joint_prob = joint_counts / joint_counts.sum()

    # Normalize using: P(M_{i,j}) / (P(S_i) * P(R_j))
    normalized_probs = {
        (s, r): joint_prob.loc[s, r] / (sender_counts[s] * receiver_counts[r]) for s, r in joint_prob.index
    }

    # Display results
    print("Raw Message Probabilities:\n", joint_prob)
    print("\nNormalized Probabilities:\n", normalized_probs)


if __name__ == "__main__":
    main()
