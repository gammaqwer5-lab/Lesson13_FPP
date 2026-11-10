using FileProcessingPipeline.Models;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;

namespace FileProcessingPipeline.Services;
// Это центральный класс практического проекта.
// Здесь мы разделяем три обязанности:
// 1. Создание заданий.
// 2. Выполнение заданий независимыми обработчиками.
// 3. Сбор результатов.
public static class PipelineProcessor
{
    public static async Task<AnalysisReport> ProcessAsync(
        IEnumerable<FileTask> tasks,
        int workerCount)
    {
        ArgumentNullException.ThrowIfNull(tasks);

        if (workerCount < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(workerCount));
        }

        var taskList = tasks.ToList();

        var results = new ConcurrentBag<FileAnalysisResult>();

        var errors = new ConcurrentBag<FileProcessingError>();
         
        // TODO:
        // Добавить механизм передачи заданий
        // между производителем и обработчиками.
        var channel = Channel.CreateBounded<FileTask>(10);

        var stopwatch = Stopwatch.StartNew();

        Task[] workers = Enumerable
            .Range(1, workerCount)
             .Select(async workerId =>
             {
                 await foreach (var task in channel.Reader.ReadAllAsync())
                 {
                     await WorkerAsync(workerId, task, results, errors);
                 }
             })
            .ToArray();
        // TODO:
        // Запустить workerCount обработчиков.
        foreach (var task in taskList)
        {
            await channel.Writer.WriteAsync(task);
        }
        // TODO:
        // Организовать получение и обработку заданий.
        channel.Writer.TryComplete();
        // TODO:
        // Дождаться завершения всех обработчиков.
        await Task.WhenAll(workers);
        // TODO:
        // Измерить общее время выполнения.
        stopwatch.Stop();

        return ReportService.Create(
            $"Pipeline ({workerCount} workers)",
            taskList.Count,
            results.ToList(),
            errors.ToList(),
            workerCount,
            stopwatch.ElapsedMilliseconds);
    }
    // уже реализует обработку одного задания
    // Но пока не определено, откуда обработчик будет получать следующее задание
    private static async Task WorkerAsync(
        int workerId,
        FileTask task,
        ConcurrentBag<FileAnalysisResult> results,
        ConcurrentBag<FileProcessingError> errors)
    {
        try
        {
            Console.WriteLine(
                $"[WORKER {workerId}] Начал: {task.FilePath}");

            var result = await FileAnalyzer.AnalyzeAsync(
                task,
                workerId);

            results.Add(result);

            Console.WriteLine(
                $"[WORKER {workerId}] Завершил: {result.FileName}");
        }
        catch (IOException ex)
        {
            errors.Add(new FileProcessingError(
                task.Id,
                Path.GetFileName(task.FilePath),
                nameof(IOException),
                ex.Message));
        }
        catch (UnauthorizedAccessException ex)
        {
            errors.Add(new FileProcessingError(
                task.Id,
                Path.GetFileName(task.FilePath),
                nameof(UnauthorizedAccessException),
                ex.Message));
        }
    }
}