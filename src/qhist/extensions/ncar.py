from pbsparse import PbsRecord

class DerechoRecord(PbsRecord):
    """Child class of PbsRecord that adds power metrics specific to Derecho supercomputer"""

    def process_record(self):
        # First perform general processing from parent class
        super().process_record()

        if hasattr(self, "resources_used"):
            for energy_var in ("cpu_", "", "gpu0_", "gpu1_", "gpu2_", "gpu3_", "memory_"):
                try:
                    energy_field = "x_ncar_" + energy_var + "energy"
                    power_field = energy_var + "power"
                    self.resources_used[energy_field] = int(self.resources_used[energy_field])
                    self.resources_used[power_field] = self.resources_used[energy_field] / (self.resources_used["walltime"] * self._divisor)
                except (KeyError, ValueError, ZeroDivisionError) as e:
                    pass
