from  qt_pvp.interest_merge_funcs import merge_overlapping_interests
from qt_pvp import functions as main_funcs
from main_operator import Main
import asyncio


REG_ID = "018270348452"
START_TIME = "2025-10-29 08:20:08"
END_TIME = "2025-10-29 08:35:00"


inst = Main()
reg_info = main_funcs.get_reg_info(reg_id=REG_ID)
async def local_get_interests_async():
    await inst.login()
    interests = await inst.get_interests_async(reg_id=REG_ID, reg_info=reg_info, start_time=START_TIME, stop_time=END_TIME)
    interests = merge_overlapping_interests(interests)
    for interest in interests:
        print(interest)

if __name__ == "__main__":
    asyncio.run(local_get_interests_async())


real_points = [
    {"type": "euro", "amount": 2, "time_start": "2025-10-15 08:09:22", "time_end": "2025-10-15 08:09:22"},
]