# -*- coding: utf-8 -*-
# Time: 2019/07/04 11:00:00
# File Name: main.py

from datetime import datetime
import os
from apscheduler.schedulers.blocking import BlockingScheduler
import run_ai as ra


def run():
    print('Tick! The time is: %s' % datetime.now())
    ra.run()


if __name__ == '__main__':
    # BlockingScheduler：适用于调度程序是进程中唯一运行的进程，调用start函数会阻塞当前线程，不能立即返回
    # BackgroundScheduler：适用于调度程序在应用程序的后台运行，调用start后主线程不会阻塞
    scheduler = BlockingScheduler()
    scheduler.add_job(run, 'cron', hour=9, minute=56)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C    '))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
