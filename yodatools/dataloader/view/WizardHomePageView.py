import wx


class WizardHomePageView(wx.Panel):
    def __init__(self, parent):
        super(WizardHomePageView, self).__init__(parent)

        # Create components
        instructions_text = wx.StaticText(self, label='Load YODA file or Excel Template')  # noqa
        # self.input_file_text_ctrl = RichTextCtrl(self)
        self.input_file_text_ctrl = wx.TextCtrl(self)
        self.browse_button = wx.Button(self, label='Browse')
        self.yoda_check_box = wx.CheckBox(self, id=1, label='YODA')
        self.excel_check_box = wx.CheckBox(self, id=2, label='Excel Template (Not implemented yet)')  # noqa
        self.odm2_check_box = wx.CheckBox(self, id=3, label='ODM2 Database')
        self.sqlite_check_box = wx.CheckBox(self, id=4, label='SQLite ODM2 Database')

        # Style components
        self.input_file_text_ctrl.SetHint('Input file...')

        # Sizer
        sizer = wx.BoxSizer(wx.VERTICAL)
        input_sizer = wx.BoxSizer(wx.HORIZONTAL)
        static_box_sizer = wx.StaticBoxSizer(wx.StaticBox(self, label='Export to'), orient=wx.VERTICAL)  # noqa

        # Add components to sizer
        input_sizer.Add(self.input_file_text_ctrl, 1, wx.EXPAND | wx.ALL, 2)
        input_sizer.Add(self.browse_button, 0, wx.ALL, 0)
        static_box_sizer.Add(self.yoda_check_box, 0, flag=wx.EXPAND | wx.ALL, border=15)  # noqa
        static_box_sizer.Add(self.excel_check_box, 0, flag=wx.EXPAND | wx.ALL, border=15)  # noqa
        static_box_sizer.Add(self.odm2_check_box, 0, flag=wx.EXPAND | wx.ALL, border=15) # noqa
        static_box_sizer.Add(self.sqlite_check_box, 0, flag=wx.EXPAND | wx.ALL, border=15)  # noqa

        sizer.Add(instructions_text, 0, wx.EXPAND | wx.ALL, 5)
        sizer.Add(input_sizer, 0, wx.EXPAND | wx.ALL, 5)
        sizer.Add(static_box_sizer, 1, wx.EXPAND | wx.ALL, 5)

        self.SetSizer(sizer)
        self.Hide()

        # Bindings
        self.browse_button.Bind(wx.EVT_BUTTON, self.on_browse_button)
        self.yoda_check_box.Bind(wx.EVT_CHECKBOX, self.on_check_box)
        self.excel_check_box.Bind(wx.EVT_CHECKBOX, self.on_check_box)
        self.odm2_check_box.Bind(wx.EVT_CHECKBOX, self.on_check_box)
        self.sqlite_check_box.Bind(wx.EVT_CHECKBOX, self.on_check_box)

    def on_check_box(self, event):
        pass

    def on_browse_button(self, event):
        pass
