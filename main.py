import argparse

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="启动宝可梦璀璨宝石联机服务")
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="监听地址（局域网访问可设为 0.0.0.0，默认：127.0.0.1）",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="监听端口（默认：8000）",
    )
    args = parser.parse_args()
    uvicorn.run("server.app:app", host=args.host, port=args.port, reload=False)


if __name__ == "__main__":
    main()
