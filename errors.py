



class PipelineError(Exception):
    """Base class for all pipeline-related errors."""
    pass

class AttributeNotInitializedError(PipelineError):
    def __init__(self, message="Attribute is not initialized"):
        self.message = message
        super().__init__(self.message)

class MethodPreconditionError(PipelineError):
    def __init__(self, message="Method precondition is not met"):
        self.message = message
        super().__init__(self.message)

class SMTPConfigurationError(PipelineError):
  def __init__(self, message="SMTP IP is not set. Set the IP map table using set_smtp_ip(dataset, table)'"):
        self.message = message
        super().__init__(self.message)

class TableMappingError(PipelineError):
  def __init__(self, message="Table Mapping is not set. Set using set_table_map(dataset, table)'"):
        self.message = message
        super().__init__(self.message)


class TaskError(PipelineError):
  def __init__(self, message):
        self.message = message
        super().__init__(self.message)