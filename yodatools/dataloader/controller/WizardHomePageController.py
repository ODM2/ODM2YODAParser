# import wx
from wx import FileDialog, FD_CHANGE_DIR, ID_OK



from yodatools.dataloader.view.WizardHomePageView import WizardHomePageView


class WizardHomePageController(WizardHomePageView):
    def __init__(self, parent, title=''):
        super(WizardHomePageController, self).__init__(parent)
        self.parent = parent
        self.title = title
        self.pages_enabled = {0: True}
        self.excel_check_box.Disable()

    def on_check_box(self, event):
        # self.pages_enabled[event.GetId()] = event.IsChecked()
        self.pages_enabled[event.GetId()] = event.IsChecked()
        if True in self.pages_enabled.values()[1:5]:
            self.GetTopLevelParent().next_button.Enable()
        else:
            self.GetTopLevelParent().next_button.Disable()

    def on_browse_button(self, event):
        dialog = FileDialog(
            self,
            message='Add file',
            style=FD_CHANGE_DIR
        )

        if dialog.ShowModal() != ID_OK:
            return

        self.input_file_text_ctrl.SetValue(dialog.GetPath())
