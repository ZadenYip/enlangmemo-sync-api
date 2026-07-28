# EnLangMemo-Sync-API

## 说明

这个仓库放的是服务端和客户端共用的代码，用 ConnectRPC 协议作为传输的手段，以此搭建同步服务。

相关的生成代码以包的形式上传，然后让服务端和客户端作为依赖去引用。

## 使用


安装依赖：
```bash
pnpm install
```

安装完后，可以用 pnpm buf 调用 Buf ClI。