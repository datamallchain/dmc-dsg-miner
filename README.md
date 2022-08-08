![rust](https://img.shields.io/badge/rustc-1.57%2B-green?style=plastic)![GitHub](https://img.shields.io/github/license/datamallchain/dmchain_contract)

[Chinese](./README.zh_CN.md)

DMC DSG Miner is a file storage mining program based on [CYFS](https://github.com/buckyos/CYFS) and DMC chain. Users can use it to provide storage services to other users and obtain DMC income. It is a A DEC App on the CYFS platform, CYFS OOD and CYFS Browser must be installed and activated before using it.

## build

cd ./src

cargo build --release

Compilation will generate two executable files, dmc-dsg-miner and dmc-dsg-miner-cli. dmc-dsg-miner is a mining program and must be run on OOD as a DEC App. dmc-dsg-miner-cli is The command line of the mining program, which is needed to control the mining program, must be run on the machine where cyfs-runtime is installed.

## dmc-dsg-miner deploy

1.Copy dmc-dsg-miner to any directory of ood

2.run dmc-dsg-miner

## dmc-dsg-miner-cli usage

1.Create a low-privilege private key required for DSG operation

dmc-dsg-miner-cli create_light_auth <dmc_account> <private_key>

Since the mining program needs to continuously interact with the DMC chain, such as obtaining and responding to the user's storage challenges, these interactions require signatures to operate normally. In order to ensure the security of the user's private key, a low-privilege private key is specially set up. key only dsg-related interfaces can be called, and other interfaces such as transfers cannot be called.

2.Set dmc account and light_private_key  to DSG

dmc-dsg-miner-cli set_dmc_account <dmc_account> <light_private_key>

3.Staking DMC

dmc-dsg-miner-cli stake <dmc_account> <private_key> <amount>

 4.Mint PST

dmc-dsg-miner-cli mint <dmc_account> <private_key> <amount>

5.Sell PST

dmc-dsg-miner-cli stake <dmc_account> <private_key> <amount> <price>

6.view info

dmc-dsg-miner-cli info <dmc_account>

ps：The private key in the command parameter must be the private key above the owner permission level of the user account. The private key will only be used locally, and the program will not send it to the Internet or save it elsewhere.