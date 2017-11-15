# import wx
from wx import Panel, Button, StaticBox, ComboBox, StaticText, TextCtrl,\
    BoxSizer, StaticBoxSizer, FlexGridSizer, \
    DefaultPosition, DefaultSize, Size, EmptyString, Font, \
    ID_ANY, ALL, SIMPLE_BORDER, TAB_TRAVERSAL, VERTICAL, FLEX_GROWMODE_ALL, EXPAND, ALIGN_RIGHT, NORMAL_FONT, HORIZONTAL,\
    CB_READONLY, FULL_REPAINT_ON_RESIZE, ALIGN_CENTER_VERTICAL,  ALIGN_CENTER_HORIZONTAL, TE_PASSWORD, EVT_BUTTON, GROW




class WizardYodaPageView(Panel):
    def __init__(self, parent):
        super(WizardYodaPageView, self).__init__(parent)

        # Components
        instructions_text = StaticText(self, label='Choose a location to save YODA export')  # noqa
        self.file_text_ctrl = TextCtrl(self)
        self.browse_button = Button(self, label='Browse')

        # Style components
        self.file_text_ctrl.SetHint('Choose a directory...')

        # Sizer
        sizer = BoxSizer(HORIZONTAL)
        vertical_sizer = BoxSizer(VERTICAL)
        input_sizer = BoxSizer(HORIZONTAL)

        # Add components to vertical_sizer
        input_sizer.Add(self.file_text_ctrl, 1, EXPAND | ALL, 2)
        input_sizer.Add(self.browse_button, 0, EXPAND | ALL, 2)

        vertical_sizer.Add(instructions_text, 0, EXPAND | ALL, 2)
        vertical_sizer.Add(input_sizer, 0, EXPAND | ALIGN_CENTER_HORIZONTAL)  # noqa
        sizer.Add(vertical_sizer, 1, ALIGN_CENTER_VERTICAL | ALL, 16)

        self.SetSizer(sizer)
        self.Hide()

        # Bindings
        self.browse_button.Bind(EVT_BUTTON, self.on_browse_button)

    def on_browse_button(self, event):
        pass
