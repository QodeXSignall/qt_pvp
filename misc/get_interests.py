from qt_pvp import functions as main_funcs
from main_operator import Main


REG_ID = "108411"
START_TIME = "2025-08-17 05:32:04"
#START_TIME = "2025-08-17 10:30:00"
END_TIME = "2025-08-17 14:43:55"
#END_TIME = "2025-08-17 10:40:00"


inst = Main()
reg_info = main_funcs.get_reg_info(reg_id=REG_ID)
interests = inst.get_interests(reg_id=REG_ID, reg_info=reg_info, start_time=START_TIME, stop_time=END_TIME)

#for interest in interests:
    #print(interest["name"])


