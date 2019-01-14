import sys
import os
import wx
from yodatools.dataloader.controller.WizardController import WizardController

import pymysql  # Import pymysql when building the application so pyinstaller can find the pymysql module
import pyodbc
import psycopg2

sys.path.append('C:\\Users\\craig\\Environments\\yoda-tools\\Lib\\site-packages\\pymysql')


def main():

    app = wx.App()
    controller = WizardController(None)
    controller.CenterOnScreen()
    controller.Show()
    app.MainLoop()


if __name__ == '__main__':

    DEBUG = os.getenv('DEBUG', 'false')
    if DEBUG.lower() == 'true':
        os.environ.setdefault('DEBUG', 'true')
    else:
        os.environ.setdefault('DEBUG', 'false')

    main()
