namespace OSWS.Models.DTOs;

public class ContextOptions
{
    public int? TraceId { get; set; }
    public required Emit Emitter { get; set; }
}

public abstract class Emit { }
