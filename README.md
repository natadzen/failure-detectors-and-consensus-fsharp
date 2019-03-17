# **Failure Detection and Consensus in Distributed Systems with F\#**

**Date**: MM/dd/2019

**Author**: [Natallia Dzenisenka](https://twitter.com/nata_dzen)

## **Abstract**

This publication covers the topic of failure detectors and consensus - fundamental distributed algorithms. They are essential to enable available, fault-tolerant, and resilient distributed systems.

There are many academic papers that describe the theory behind distributed algorithms and prove their correctness, but rarely they illustrate other valuable aspects associated with practical implementations of the algorithms.

To be able to build distributed systems, it's important to understand the end-to-end process of working with distributed algorithms. This includes underlying networking and actual details of implementation of the algorithm using a programming language.

F\# is a powerful open-source programming language that provides many advantages for distributed system development. F\# consolidates useful concepts that are beneficial for concurrent and distributed programming, such as computation expressions, function composition, strong typing, type inference, and many more.

This publication demonstrates how F# can be used to build resilient distributed systems. It describes the theory behind the crucial distributed failure detector and consensus algorithms, presenting implementations of variety of failure detector algorithms and a failure-detector-based consensus algorithm.

## **Table Of Contents**

### **Part I: Theory behind Failure Detectors**

1. [Failure Detectors](Theory-Behind-Failure-Detectors.md#Failure-Detectors)
2. [Failures Are Everywhere](Theory-Behind-Failure-Detectors.md#Failures-Are-Everywhere)
3. [Discovering Failures](Theory-Behind-Failure-Detectors.md#Discovering-Failures)
4. [Failure Detectors Of The Real World](Theory-Behind-Failure-Detectors.md#Failure-Detectors-Of-The-Real-World)
5. [Problems Solved With Failure Detectors](Theory-Behind-Failure-Detectors.md#Problems-Solved-With-Failure-Detectors)
6. [Properties Of A Failure Detector](Theory-Behind-Failure-Detectors.md#Properties-Of-A-Failure-Detector)
7. [Types Of Failure Detectors](Theory-Behind-Failure-Detectors.md#Types-Of-Failure-Detectors)
8. [Failure Detectors In Asynchronous Environment](Theory-Behind-Failure-Detectors.md#Failure-Detectors-In-Asynchronous-Environment)
9. [Reducibility Of Failure Detectors](Theory-Behind-Failure-Detectors.md#Reducibility-Of-Failure-Detectors)

### **Part II: Implementation of Networking**

1. [Message Serialization](Implementation-Of-Networking.md##Message-serialization)
2. [TCP Server](Implementation-Of-Networking.md#TCP-server)
3. [UDP Server](Implementation-Of-Networking.md#UDP-server)

### **Part III: Implementation of Failure Detectors**

1. [Implementing A Node](Implementation-Of-Failure-Detectors.md#Implementing-A-Node)
2. [Ping-Ack Failure Detector](Implementation-Of-Failure-Detectors.md#Ping-Ack-Failure-Detector)
3. [Heartbeat Failure Detector](Implementation-Of-Failure-Detectors.md#Heartbeat-Failure-Detector)
4. [Heartbeat Failure Detector With Adjustable Timeout](Implementation-Of-Failure-Detectors.md#Heartbeat-Failure-Detector-With-Adjustable-Timeout)
5. [Heartbeat Failure Detector With Sliding Window](Implementation-Of-Failure-Detectors.md#Heartbeat-Failure-Detector-With-Sliding-Window)
6. [Heartbeat Failure Detector With Suspect Level](Implementation-Of-Failure-Detectors.md#Heartbeat-Failure-Detector-With-Suspect-Level)
7. [Gossipping Failure Detector](Implementation-Of-Failure-Detectors.md#Gossipping-Failure-Detector)

### **Part IV: Implementation of Consensus**

1. [Implementing Consensus Based On Failure Detectors](Implementation-Of-Consensus.md#Implementing-Consensus-Based-On-Failure-Detectors)
2. [Chandra-Toueg Consensus](Implementation-Of-Consensus.md#Chandra-Toueg-Consensus)