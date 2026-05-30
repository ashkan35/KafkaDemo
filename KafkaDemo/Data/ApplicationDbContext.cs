using KafkaDemo.Entities;
using Microsoft.EntityFrameworkCore;

namespace KafkaDemo.Data;

public class ApplicationDbContext(DbContextOptions<ApplicationDbContext> options) : DbContext(options)
{
    public DbSet<UserLoggedInEvent> UserLoggedInEvents => Set<UserLoggedInEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<UserLoggedInEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id)
                .HasMaxLength(26)
                .ValueGeneratedNever();

            entity.Property(e => e.UserId)
                .IsRequired();

            entity.Property(e => e.UserName)
                .IsRequired();

            entity.Property(e => e.LoggedInAt)
                .HasColumnType("timestamp without time zone");

            entity.Property(e => e.Description)
                .IsRequired()
                .HasMaxLength(10 * 1024);
        });
    }
}
