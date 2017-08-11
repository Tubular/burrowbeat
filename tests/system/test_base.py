from burrowbeat import BaseTest

import os


class Test(BaseTest):

    def test_base(self):
        """
        Basic test with exiting Burrowbeat normally
        """
        self.render_config_template(
            path=os.path.abspath(self.working_dir) + "/log/*"
        )

        burrowbeat_proc = self.start_beat()
        self.wait_until(lambda: self.log_contains("burrowbeat is running"))
        exit_code = burrowbeat_proc.kill_and_wait()
        assert exit_code == 0
