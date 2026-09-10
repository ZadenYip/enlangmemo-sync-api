# EnLangMemo-Sync-API

## 说明

这个仓库放的是服务端和客户端共用的代码，用 ConnectRPC 协议作为传输的手段，以此搭建同步服务。

相关的生成代码以包的形式上传，然后让服务端和客户端作为依赖去引用。

## 使用

安装依赖：
```bash
pnpm install
```

安装完后，可以用 pnpm buf 调用 Buf CLI 来生成代码，下面是根据 proto 文件生成代码的命令：

```bash
pnpm buf generate
```

生成完代码要发布新版本，记得修改 packges/ts/package.json 和 根目录的 package.json 的版本号。

接着用打 tag，一个打 packages/ts/vX.X.X 的 tag，一个打根目录的 packages/go/vX.X.X 的 tag，然后 push tag，剩下的会交给 CI 脚本会自动发布到 npm 和 go module。