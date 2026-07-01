# Introduction to OmniStateStore

## Latest Updates

- [2026-06-30] Released OmniStateStore 1.3.0. By tuning the compression strategies across different Sorted String Table (SST) levels in RocksDB and optimizing software-based compression algorithms, the compression and decompression overhead during state compaction is reduced, thereby improving end-to-end application throughput.
- [2026-03-30] Released OmniStateStore 1.2.0. This version integrates Flink and RocksDB as plugins to improve the performance of Flink stateful test cases. It introduces lightweight modifications to Flink. By leveraging state caching and state filtering techniques, it reduces Flink's access to RocksDB and improves I/O performance for stateful workloads. The architecture of version 1.2.0 has been revised and is independent of versions 1.1.0 and 1.0.0.
- [2025-12-30] Released OmniStateStore 1.1.0. Added support for interconnection with the Flink metric framework to implement common metrics, as well as persistent storage of priority queues and KV separated storage.
- [2025-06-30] Released OmniStateStore 1.0.0. It introduces a new state storage technology to improve the I/O performance of Flink in big data scenarios.

## Introduction to the Project

### Overview

The big data features of OmniRuntime are presented in the form of plugins to improve the performance of data loading, computing, and exchange from end to end.

Data volumes generated from Internet services have been growing much faster than CPUs' computing power. The open-source big data ecosystem is also developing on a fast track. However, diversified computing engines and open-source components make it difficult to improve data processing performance throughout the lifecycle. Different big data engines use their own unique tuning policies and technologies to improve performance and efficiency. Some tuning items may be applied across multiple engines, which may cause resource contention and conflicts, reducing overall computing performance.

OmniRuntime consists of a series of features provided by Kunpeng BoostKit for Big Data in terms of application acceleration. It aims to improve the performance of end-to-end data loading, computing, and exchange through plugins, thereby improving the performance of big data analytics.

OmniStateStore is part of the OmniRuntime feature set. It introduces techniques such as state caching, state filtering, and software-based compression optimization to reduce Flink's access to RocksDB and the associated I/O overhead. As a result, the compression and decompression overhead during state compaction is reduced, and the end-to-end performance of Flink stateful workloads is improved. It has been adapted to the following open-source components and versions:

- Flink1.16.3 + RocksDB6.20.3 (26.0.RC1)

## Feature Description

State store is an important feature of Flink and is mainly implemented by the state backend. As the volume of state data grows, the performance of state storage comes under pressure. OmniStateStore introduces lightweight modifications to Flink. It leverages techniques such as state caching, state filtering, and software-based compression optimization to reduce Flink's reliance on RocksDB and improve end-to-end performance.

OmniStateStore serves as a middleware layer between Flink and RocksDB. It incorporates the dynamic filter technique, Flink-aware state caching, Merge read/write optimization, and LZ4 software-based compression optimization. The following figure illustrates the overall architecture.

- Dynamic filter: A state-prefix filter is used to eliminate redundant drive lookup operations during MapState range queries. For workloads that require only point reads and writes, the MemTable data structure is replaced with a HashLinkedList to improve the efficiency of point operations.
- Flink-aware state caching: A ValueState cache is employed to preferentially aggregate state for the same key in memory, thereby reducing the frequency of accesses to RocksDB. In addition, a Join operator cache is used to minimize repeated range queries over state during dual-stream join operations.
- Merge read/write optimization: The RocksDB Merge operator is used to replace the read-modify-write (RMW) operations in Flink SQL and the DataStream API.
- LZ4 software-based compression optimization: By switching the compression format of RocksDB Level 0/Level 1 to LZ4 and optimizing the LZ4 software-based compression algorithm, the efficiency of state compression and decompression is improved.

**Figure 1** Overall OmniStateStore architecture

<a href="./docs/en/figures/Overall_OmniStateStore_architecture_design.png"><img src="./docs/en/figures/Overall_OmniStateStore_architecture_design.png" alt="Overall_OmniStateStore_architecture_design" width="800" /></a>

### Typical Deployments

As a plugin bridging Flink and RocksDB, OmniStateStore is deployed alongside Flink. It supports multiple deployment modes, including Yarn, standalone, and containerized environments.

In a typical deployment scenario, OmniStateStore is deployed across three Docker containers, each allocated 8 cores and 32 GB of memory. One container runs the JobManager, while each of the remaining two containers hosts four TaskManagers. The Job Manager is allocated 8 GB of memory, and each Task Manager is allocated two TaskSlots and 8 GB of memory.

### Application Scenarios

OmniStateStore is designed for stateful scenarios in Apache Flink stream processing tasks. As the volume of state data increases, I/O performance often becomes a major bottleneck. Typical scenarios include:

- Real-time big data processing: In tasks such as real-time extract-transform-load (ETL), streaming aggregation, and windowed computation, the state grows continuously with incoming data.
- Complex event processing and stateful stream computing: Large-scale states (such as user session tracing and real-time risk control modeling) needs to be maintained for a long time.

By leveraging techniques such as state caching, state filtering, and software-based compression optimization, OmniStateStore reduces the frequency of Flink's access to RocksDB and lowers the overhead of state compression and decompression, thereby improving the end-to-end throughput of stateful jobs. OmniStateStore is compatible with openEuler 22.03 LTS SP3 and supports the Flink 1.16.3 + RocksDB 6.20.3 architecture.

## Constraints

The performance improvement achieved by OmniStateStore depends on the proportion of RocksDB usage and the type of state operations in the test case. When the RocksDB proportion is low, performance is not adversely affected.

As an acceleration plugin for Flink, OmniStateStore is compatible with the Huawei Kunpeng platform and can also run on general-purpose x86 servers.

## Directory Structure

The full project directory structure is as follows:

```text
OmniStateStore/                       # Project root directory
|—— conf/                             # Flink configuration file
|   └── flink-conf.yaml               # Example Flink parameter configuration for enabling OmniStateStore
|—— docs/                             # Project document directory
|   |—— en                            # Document directory
|   |   |—— figures                   # Images in documents
|   |   |—— quick_start.md            # Quick Start
|   |   |—— release_notes.md          # OmniStateStore Release Notes
|   |   |—— installation_guide.md     # OmniStateStore Installation Guide
|   |   |—— user_guide.md             # OmniStateStore User Guide
|   |   |—— best_practices.md         # OmniStateStore Scenario-specific Application Best Practices
|   |   |—— design_guide.md           # OmniStateStore Design Guide
|   |   └── faq.md                    # OmniStateStore Installation FAQs
|—— cpp/                              # Core code of OmniStateStore – C++
|—— java/                             # Core code of OmniStateStore – Java
|—— scripts/                          # Script for automatically building OmniStateStore code
|   |—— build.sh                      # Automatic build script
|   └── Makefile.patch                # RocksDB compilation script patch. It is applied in the RocksDB source code to complete the compilation.
|—— .gitignore                        # Git configuration of the project
|—— LICENSE                           # LICENSE
└── README_en.md                      # README
```

## Release Notes

For details about feature changes in each version, see [Release Notes](./docs/en/release_notes.md).

## Environment Deployment

For details about the environment dependencies and installation methods of OmniStateStore, see [Installation Guide](./docs/en/installation_guide.md).

## Quick Start

For instructions on quickly verifying whether OmniStateStore is active and its performance improvements, see [Quick Start](./docs/en/quick_start.md).

## Related Documents

**Table 1** Related documents
<table>
  <thead>
    <tr>
      <th style="text-align: left;">Software Package</th>     
      <th style="text-align: left;">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>      
      <td style="text-align: left;"><a href="./docs/en/quick_start.md">Quick Start</a></td>
      <td style="text-align: left;">Provides guidance on how to quickly enable and verify the OmniStateStore feature.</td>
    </tr>
    <tr>      
      <td style="text-align: left;"><a href="./docs/en/release_notes.md">Release Notes</a></td>
      <td style="text-align: left;">Provides basic information and feature updates for each OmniStateStore version.</td>
    </tr>
    <tr>      
      <td style="text-align: left;"><a href="./docs/en/design_guide.md">Design Guide</a></td>
      <td style="text-align: left;">Provides OmniStateStore feature description.</td>
    </tr>
    <tr>     
      <td style="text-align: left;"><a href="./docs/en/installation_guide.md">Installation Guide</a></td>
      <td style="text-align: left;">Provides detailed guidance for installing OmniStateStore.</td>
    </tr>
    <tr>      
      <td style="text-align: left;"><a href="./docs/en/user_guide.md">User Guide</a></td>
      <td style="text-align: left;">Provides detailed guidance for using OmniStateStore.</td>
    </tr>
    <tr>      
      <td style="text-align: left;"><a href="./docs/en/best_practices.md">Best Practices</a></td>
      <td style="text-align: left;">Provides OmniStateStore practice cases.</td>
    </tr>
   <tr>      
      <td style="text-align: left;"><a href="./docs/zh/faq.md">FAQs</a></td>
      <td style="text-align: left;">Provides solutions to issues that may arise during the installation and operation of OmniStateStore.</td>
    </tr>
    <tr>      
      <td style="text-align: left;">Video Tutorials: <a href="https://www.hikunpeng.com/document/video-detail/2644?page=home&type=video&title=OmniRuntime%E7%89%B9%E6%80%A7%E5%A4%A7%E6%8F%AD%E7%A7%98">A Deep Dive into OmniRuntime Features</a></td>
      <td style="text-align: left;">Helps better use OmniRuntime on Kunpeng servers.</td>
    </tr>
  </tbody>
</table>

## Security Declaration

### Routine Antivirus Software Check

Periodically scan clusters and Flink components for viruses. This protects clusters from viruses, malicious code, spyware, and malicious programs, reducing risks such as system breakdown and information leakage. Mainstream antivirus software is recommended for antivirus check.

### Log Control

- Check whether the system can limit the size of a single log file.
- Check whether there is a mechanism for clearing logs when the log space is used up.

### Vulnerability Fixing

To ensure the security of the production environment and reduce the risk of attacks, enable the firewall and periodically fix the following vulnerabilities:

- OS vulnerabilities
- JDK vulnerabilities
- Flink vulnerabilities
- ZooKeeper vulnerabilities
- Kerberos vulnerabilities
- OpenSSL vulnerabilities
- Vulnerabilities in other components

For example, CVE-2021037317:

Vulnerability description: Netty 4.1.17 has two Content-Length HTTP headers that may be confused. The vulnerability ID is CVE-2021037317.
The system uses the hdfs-ceph (version 3.2.0) service as the storage object with decoupled storage and compute. This service depends on `aws-java-sdk-bundle-1.11.375.jar` and involves this vulnerability. You are advised to update the vulnerability patch in a timely manner to prevent hacker attacks.

Impact scope: Netty 4.1.68 and earlier versions.

Handling suggestion: The vendor has released an upgrade patch to fix the vulnerability. For details, visit [GitHub](https://github.com/netty/netty/security/advisories/GHSA-9vjp-v76f-g363).

### SSH Hardening

During the installation and deployment, you need to connect to the server through SSH. The root user has all the operation permissions. Logging in to the server as the root user may pose security risks. You are advised to log in to the server as a common user for installation and deployment and disable root user login using SSH to improve system security.

Check the `PermitRootLogin` configuration item in `/etc/ssh/sshd_config`.

- If the value is no, root user login using SSH is disabled.
- If the value is `yes`, change it to `no`.

### Public Network Address Statement

**Table 2** Public Network Address Statement
<table>
  <tbody>
    <tr>
      <td style="text-align: left;">Open-Source Software / Third-Party Software</td>
      <td style="text-align: left;">GCC, Maven</td>
    </tr>
    <tr>
      <td style="text-align: left;">Type</td>
      <td style="text-align: left;">Open-Source Software</td>
    </tr>
    <tr>
      <td style="text-align: left;">Public IP Address / Public URL / Domain Name / Email Address</td>
      <td style="text-align: left;"><a href="https://gcc.gnu.org/bugs/">https://gcc.gnu.org/bugs/</a></td>
    </tr>
    <tr>
      <td style="text-align: left;">File Type</td>
      <td style="text-align: left;">Binary</td>
    </tr>
    <tr>
      <td style="text-align: left;">File Name</td>
      <td style="text-align: left;">flink-alg-falcon.jar, librocksdb.so.6</td>
    </tr>
    <tr>
      <td style="text-align: left;">Usage Description</td>
      <td style="text-align: left;">This email address is the official address of the open-source component GCC, and is used only to compile the open-source component. This email address is not used inside this product.</td>
    </tr>
    <tr>
      <td style="text-align: left;">Software Package</td>
      <td style="text-align: left;">BoostKit-omniruntime-omniStateStore-1.3.0.zip</td>
    </tr>
  </tbody>
</table>

## Disclaimer

**To OmniStateStore Users**

- This tool is intended solely for debugging and development. You are responsible for any risks and should carefully review the following information:

  - Data processing and deletion: Users are responsible for managing and deleting any data generated while using this tool. You are advised to promptly delete any related data after use to prevent information leaks.
  - Data confidentiality and transmission: Users understand and agree not to share or transmit any data generated by this tool. Neither the tool nor its developers are responsible for any information leaks, data breaches, or other negative consequences.
  - User input security: Users are responsible for the security of any commands they enter and for any risks or losses resulting from improper input. The tool and its developers are not liable for issues caused by incorrect command usage.
- Disclaimer scope: This disclaimer applies to all individuals and entities using this tool. By using the tool, you acknowledge and accept this statement and assume all risks and responsibilities arising from its use. If you do not agree, please stop using the tool immediately.
- Before using this tool, please **read and understand the preceding disclaimer**. If you have any questions, contact the developer.

**To Data Owners**

If you do not want your model or dataset to be mentioned in OmniStateStore, or if you wish to update its description, please submit an issue on GitCode. We will delete or update your description according to your request. Thank you for your understanding and contribution to OmniStateStore.

## License

For OmniStateStore's license information, see [LICENSE](./LICENSE).

## Contribution Statement

1. Submit an error report: If you discover a vulnerability in OmniStateStore that is not a security issue, first search the **Issues** in the OmniStateStore repository to avoid submitting duplicates. If the vulnerability is not listed, create a new issue. If you discover a security-related issue, do not disclose it publicly. Please refer to the security handling guidelines for details. All error reports must include complete information about the issue.

2. Handling security issues: For guidance on handling security issues in this project, please contact the core team via email for instructions.

3. Resolving existing issues: Review the issue list of the repository to identify issues that need attention, and attempt to resolve them.

4. Proposing new features: Use the **Feature** label when creating an issue for a new feature. We will review and confirm proposals periodically.

5. How to contribute:

&emsp; a. Fork the repository of the project.

&emsp; b. Clone it to your local machine.

&emsp; c. Create a development branch.

&emsp; d. Conduct local testing. All unit tests, including any new test cases, must pass before submission.

&emsp; e. Submit your code.

&emsp; f. Create a pull request.

&emsp; g. Code review: Modify the code according to review comments and resubmit your changes. This process may involve multiple rounds of iterations.

&emsp; h. After your PR is approved by the required number of reviewers, the committer will conduct the final review.

&emsp; i. After your PR is approved and all tests pass, the CI system will merge it into the main branch of the project.

## Suggestions and Feedback

You are welcome to contribute to the community. If you have any questions or suggestions, please submit an issue. We will respond as soon as possible. Thank you for your support.

## Acknowledgments

OmniStateStore is jointly developed by the following Huawei departments:

- Technology Development Dept, Computing Product Line
- Kunpeng Computing BoostKit Development Dept

Thank you to everyone in the community for your PRs. We warmly welcome contributions to OmniStateStore!
