![rust](https://img.shields.io/badge/rustc-1.57%2B-green?style=plastic)![GitHub](https://img.shields.io/github/license/datamallchain/dmchain_contract)

DMC DSG Miner是基于[CYFS平台](https://github.com/buckyos/CYFS)和DMC链的文件存储挖矿程序，用户可以通过它来给其它用户提供存储服务并且获取DMC收益，它是CYFS平台上的一个DEC App，使用它以前必须安装CYFS OOD和CYFS Browser并且激活。

## 编译

cd ./src

cargo build --release

编译将生成dmc-dsg-miner和dmc-dsg-miner-cli两个可执行文件，dmc-dsg-miner为挖矿程序，必须做为DEC App运行在OOD上，dmc-dsg-miner-cli为挖矿程序命令行，需要通过它来控制挖矿程序，必须运行在安装cyfs-runtime的机器上。

## dmc-dsg-miner部署

1.拷贝dmc-dsg-miner到ood任何目录

2.运行dmc-dsg-miner

## dmc-dsg-miner-cli使用

1.创建DSG运行所需低权限私钥

dmc-dsg-miner-cli create_light_auth <dmc_account> <private_key>

2.质押DMC

dmc-dsg-miner-cli stake <dmc_account> <private_key> <amount>

 3.铸造PST

dmc-dsg-miner-cli mint <dmc_account> <private_key> <amount>

4.售卖PST

dmc-dsg-miner-cli stake <dmc_account> <private_key> <amount> <price>

5.查看相关信息

dmc-dsg-miner-cli info <dmc_account>