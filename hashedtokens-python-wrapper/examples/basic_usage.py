import asyncio

import hashedtokens as ht


async def main() -> None:
    result = await ht.request(
        "https://example.com/api/posts",
        {"page": 1},
        "GET",
        timeout=15,
        collection="demo",
        tags=["blog", "read"],
    )
    print(result.response_obj.status_code)
    print(result.ht_response)


if __name__ == "__main__":
    asyncio.run(main())
