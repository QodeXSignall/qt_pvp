from  qt_pvp.interest_merge_funcs import merge_overlapping_interests
from qt_pvp import functions as main_funcs
from main_operator import Main
import asyncio


#K630AX702_2025.10.30 10.05.08-10.16.49

#10:05:07
# 10:16:42
REG_ID = "108411"
START_TIME = "2025-11-09 11:56:40"
END_TIME = "2025-11-09 11:59:59"


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