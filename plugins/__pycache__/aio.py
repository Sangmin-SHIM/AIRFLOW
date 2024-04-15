import aiohttp
import aiofiles

async def fetch_url(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                print("download success")
                return await response.read()

            else:
                print("failed")
                
async def save_file(save_file_path, response_read: bytes):
    f = await aiofiles.open(save_file_path, mode="wb")
    await f.write(await response_read)
    await f.close()