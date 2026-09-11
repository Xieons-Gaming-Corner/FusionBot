using PKHeX.Core;
using SysBot.Base;
using SysBot.Pokemon.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TwitchLib.Client;
using TwitchLib.Client.Events;
using TwitchLib.Client.Models;
using TwitchLib.Communication.Clients;
using TwitchLib.Communication.Events;
using TwitchLib.Communication.Models;

namespace SysBot.Pokemon.Twitch;

public class TwitchBot<T> : IChatBot where T : PKM, new()
{
    internal static readonly List<TwitchQueue<T>> QueuePool = new();
    private static readonly Dictionary<ulong, DateTime> UserLastCommand = new();
    private static readonly object UserCommandLock = new();
    private static int nextTradeId = Random.Shared.Next(100_000_000, 900_000_000);

    public static PokeTradeHub<T> Hub = default!;
    private readonly PokeTradeHubConfig Config;
    private readonly TwitchSettings Settings;

    private TwitchClient? client;
    private bool isConnected;
    private bool isConnecting;
    private readonly object connectionLock = new();
    private CancellationToken cancellationToken;
    private Action<string>? echoForwarder;
    private System.Timers.Timer? userCommandCleanupTimer;

    private TaskCompletionSource<bool>? connectedTcs;
    private TaskCompletionSource<bool>? joinedChannelTcs;
    private string? configuredChannel;

    private EventHandler<OnMessageSentArgs>? logMessageSent;
    private EventHandler<OnWhisperSentArgs>? logWhisperSent;
    private EventHandler<OnMessageThrottledEventArgs>? logMessageThrottled;
    private EventHandler<OnWhisperThrottledEventArgs>? logWhisperThrottled;
    private EventHandler<OnErrorEventArgs>? logError;

    public TwitchBot(TwitchSettings settings, PokeTradeHubConfig config)
    {
        Settings = settings;
        Config = config;

        userCommandCleanupTimer = new System.Timers.Timer(TimeSpan.FromMinutes(10).TotalMilliseconds);
        userCommandCleanupTimer.Elapsed += CleanupUserCommands;
        userCommandCleanupTimer.AutoReset = true;
        userCommandCleanupTimer.Start();
    }

    public bool IsConnected => isConnected && client?.IsConnected == true;

    public async Task StartAsync(CancellationToken token)
    {
        cancellationToken = token;

        try
        {
            if (string.IsNullOrWhiteSpace(Settings.Token) || string.IsNullOrWhiteSpace(Settings.Channel))
            {
                LogUtil.LogError("Twitch Token or Channel not configured - Twitch Bot will be skipped", nameof(TwitchBot<T>));
                return;
            }

            if (string.IsNullOrWhiteSpace(Settings.Username))
            {
                LogUtil.LogError("Twitch Username not configured - Twitch Bot will be skipped", nameof(TwitchBot<T>));
                return;
            }

            await ConnectWithRetry();
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            LogUtil.LogInfo("Twitch Bot startup was cancelled", nameof(TwitchBot<T>));
        }
        catch (Exception ex)
        {
            LogUtil.LogError($"Twitch Bot could not be started: {ex.Message} - Bot will continue without Twitch", nameof(TwitchBot<T>));
        }
    }

    private async Task ConnectWithRetry(int maxRetries = 10)
    {
        lock (connectionLock)
        {
            if (isConnecting || isConnected)
                return;

            isConnecting = true;
        }

        try
        {
            for (var attempt = 1; attempt <= maxRetries; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    LogUtil.LogInfo($"Twitch connection attempt {attempt}/{maxRetries}...", nameof(TwitchBot<T>));
                    await ConnectInternal();
                    LogUtil.LogInfo("Twitch Bot successfully connected and joined its configured channel!", nameof(TwitchBot<T>));
                    return;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    isConnected = false;
                    CleanupClientOnly();

                    LogUtil.LogError($"Twitch connection attempt {attempt} failed: {ex.Message}", nameof(TwitchBot<T>));

                    if (attempt < maxRetries)
                    {
                        var delay = TimeSpan.FromSeconds(5 * attempt);
                        LogUtil.LogInfo($"Waiting {delay.TotalSeconds} seconds before next attempt...", nameof(TwitchBot<T>));
                        await Task.Delay(delay, cancellationToken);
                    }
                }
            }

            LogUtil.LogError($"All {maxRetries} Twitch connection attempts failed - Twitch will be restarted by supervisor", nameof(TwitchBot<T>));
            isConnected = false;
        }
        finally
        {
            lock (connectionLock)
                isConnecting = false;
        }
    }

    private async Task ConnectInternal()
    {
        CleanupClientOnly();

        var username = Settings.Username.Trim().ToLowerInvariant();
        var channel = Settings.Channel.Trim().TrimStart('#').ToLowerInvariant();
        var token = Settings.Token.Trim();

        if (!token.StartsWith("oauth:", StringComparison.OrdinalIgnoreCase))
            token = $"oauth:{token}";

        if (string.IsNullOrWhiteSpace(username))
            throw new InvalidOperationException("Twitch Username is empty.");

        if (string.IsNullOrWhiteSpace(channel))
            throw new InvalidOperationException("Twitch Channel is empty.");

        if (token.Equals("oauth:", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Twitch OAuth token is empty.");

        configuredChannel = channel;
        connectedTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        joinedChannelTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var clientOptions = new ClientOptions
        {
            MessagesAllowedInPeriod = Settings.ThrottleMessages,
            ThrottlingPeriod = TimeSpan.FromSeconds(Settings.ThrottleSeconds),
            WhispersAllowedInPeriod = Settings.ThrottleWhispers,
            WhisperThrottlingPeriod = TimeSpan.FromSeconds(Settings.ThrottleWhispersSeconds),
        };

        client = new TwitchClient(new WebSocketClient(clientOptions));

        var commandPrefix = Settings.CommandPrefix == default
            ? '!'
            : Settings.CommandPrefix;

        client.Initialize(new ConnectionCredentials(username, token), channel, commandPrefix, commandPrefix);

        client.OnLog += OnLog;
        client.OnJoinedChannel += OnJoinedChannel;
        client.OnMessageReceived += OnMessageReceived;
        client.OnWhisperReceived += OnWhisperReceived;
        client.OnChatCommandReceived += OnChatCommandReceived;
        client.OnWhisperCommandReceived += OnWhisperCommandReceived;
        client.OnConnected += OnConnected;
        client.OnIncorrectLogin += OnIncorrectLogin;
        client.OnConnectionError += OnConnectionError;
        client.OnDisconnected += OnDisconnected;
        client.OnFailureToReceiveJoinConfirmation += OnFailureToReceiveJoinConfirmation;
        client.OnLeftChannel += OnLeftChannel;

        logMessageSent = (_, e) => LogUtil.LogText($"[{client?.TwitchUsername}] - Message Sent in {e.SentMessage.Channel}: {e.SentMessage.Message}");
        logWhisperSent = (_, e) => LogUtil.LogText($"[{client?.TwitchUsername}] - Whisper Sent to @{e.Receiver}: {e.Message}");
        logMessageThrottled = (_, e) => LogUtil.LogError($"Message Throttled: {e.Message}", nameof(TwitchBot<T>));
        logWhisperThrottled = (_, e) => LogUtil.LogError($"Whisper Throttled: {e.Message}", nameof(TwitchBot<T>));
        logError = (_, e) => LogUtil.LogError(e.Exception.ToString(), nameof(TwitchBot<T>));

        client.OnMessageSent += logMessageSent;
        client.OnWhisperSent += logWhisperSent;
        client.OnMessageThrottled += logMessageThrottled;
        client.OnWhisperThrottled += logWhisperThrottled;
        client.OnError += logError;

        echoForwarder = SendMessage;
        client.Connect();

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(30));

        try
        {
            await connectedTcs.Task.WaitAsync(timeoutCts.Token);
            await joinedChannelTcs.Task.WaitAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException($"Timed out waiting for Twitch login and channel join confirmation for '{channel}'.");
        }

        if (echoForwarder != null && !EchoUtil.Forwarders.Contains(echoForwarder))
            EchoUtil.Forwarders.Add(echoForwarder);

        isConnected = true;
    }

    private void OnConnected(object? sender, OnConnectedArgs e)
    {
        LogUtil.LogInfo($"Twitch authenticated as '{client?.TwitchUsername}' and connected. Auto-join channel: '{e.AutoJoinChannel}'.", nameof(TwitchBot<T>));
        connectedTcs?.TrySetResult(true);
    }

    private void OnIncorrectLogin(object? sender, OnIncorrectLoginArgs e)
    {
        var error = new InvalidOperationException(
            "Twitch login failed. Verify that Username matches the Twitch account that created the OAuth token, then generate a new token if necessary.",
            e.Exception);

        LogUtil.LogError(error.Message, nameof(TwitchBot<T>));
        isConnected = false;
        connectedTcs?.TrySetException(error);
        joinedChannelTcs?.TrySetException(error);
    }

    private void OnConnectionError(object? sender, OnConnectionErrorArgs e)
    {
        var error = new InvalidOperationException($"Twitch connection error: {e.Error.Message}");
        LogUtil.LogError(error.Message, nameof(TwitchBot<T>));
        isConnected = false;
        connectedTcs?.TrySetException(error);
        joinedChannelTcs?.TrySetException(error);
    }

    private void OnDisconnected(object? sender, OnDisconnectedEventArgs e)
    {
        LogUtil.LogInfo("Twitch connection disconnected", nameof(TwitchBot<T>));
        isConnected = false;

        if (isConnecting)
        {
            var error = new InvalidOperationException("Twitch disconnected before the connection process completed.");
            connectedTcs?.TrySetException(error);
            joinedChannelTcs?.TrySetException(error);
        }
    }

    private void OnFailureToReceiveJoinConfirmation(object? sender, OnFailureToReceiveJoinConfirmationArgs e)
    {
        var error = new InvalidOperationException(
            $"Twitch did not confirm joining channel '{configuredChannel}'. {e.Exception}");

        LogUtil.LogError(error.Message, nameof(TwitchBot<T>));
        isConnected = false;
        joinedChannelTcs?.TrySetException(error);
    }

    private void OnLog(object? sender, OnLogArgs e)
    {
        var data = e.Data?.ToLowerInvariant() ?? string.Empty;

        if (data.Contains("ping") || data.Contains("pong") ||
            data.Contains("privmsg") || data.Contains("usernotice") ||
            data.Contains("roomstate") || data.Contains("userstate"))
        {
            return;
        }

        if (data.Contains("error") || data.Contains("disconnect") ||
            data.Contains("connect") || data.Contains("join") ||
            data.Contains("notice"))
        {
            LogUtil.LogInfo($"Twitch: {e.Data}", nameof(TwitchBot<T>));
        }
    }

    private void OnJoinedChannel(object? sender, OnJoinedChannelArgs e)
    {
        LogUtil.LogInfo($"Twitch Bot joined channel: {e.Channel}", nameof(TwitchBot<T>));

        if (string.Equals(e.Channel.TrimStart('#'), configuredChannel, StringComparison.OrdinalIgnoreCase))
            joinedChannelTcs?.TrySetResult(true);
    }

    private void OnLeftChannel(object? sender, OnLeftChannelArgs e)
    {
        LogUtil.LogText($"[{client?.TwitchUsername}] - Left channel {e.Channel}");

        try
        {
            if (client?.IsConnected == true && !string.IsNullOrWhiteSpace(configuredChannel))
                client.JoinChannel(configuredChannel);
        }
        catch (Exception ex)
        {
            LogUtil.LogError($"Failed to rejoin channel {e.Channel}: {ex.Message}", nameof(TwitchBot<T>));
        }
    }

    private void OnMessageReceived(object? sender, OnMessageReceivedArgs e)
    {
        LogUtil.LogText($"[{client?.TwitchUsername}] - @{e.ChatMessage.Username}: {e.ChatMessage.Message}");
    }

    private void OnWhisperReceived(object? sender, OnWhisperReceivedArgs e)
    {
        LogUtil.LogText($"[{client?.TwitchUsername}] - @{e.WhisperMessage.Username}: {e.WhisperMessage.Message}");
    }

    private void OnChatCommandReceived(object? sender, OnChatCommandReceivedArgs e)
    {
        if (!Settings.AllowCommandsViaChannel)
            return;

        var message = e.Command.ChatMessage;
        var command = e.Command.CommandText.ToLowerInvariant();
        var arguments = e.Command.ArgumentsAsString;
        var response = HandleCommand(message, command, arguments, whisper: false);

        if (!string.IsNullOrWhiteSpace(response))
            SendMessage(response);
    }

    private void OnWhisperCommandReceived(object? sender, OnWhisperCommandReceivedArgs e)
    {
        if (!Settings.AllowCommandsViaWhisper)
            return;

        var message = e.Command.WhisperMessage;
        var command = e.Command.CommandText.ToLowerInvariant();
        var arguments = e.Command.ArgumentsAsString;
        var response = HandleCommand(message, command, arguments, whisper: true);

        if (!string.IsNullOrWhiteSpace(response))
            SendWhisper(message.Username, response);
    }

    internal static TradeQueueInfo<T> Info => Hub.Queues.Info;

    public void SendMessage(string message)
    {
        try
        {
            if (IsConnected && !string.IsNullOrWhiteSpace(configuredChannel))
                client!.SendMessage(configuredChannel, message);
        }
        catch (Exception ex)
        {
            LogUtil.LogError($"Error sending Twitch message: {ex.Message}", nameof(TwitchBot<T>));
        }
    }

    private void SendWhisper(string user, string message)
    {
        try
        {
            if (IsConnected)
                client!.SendWhisper(user, message);
        }
        catch (Exception ex)
        {
            LogUtil.LogError($"Error sending Twitch whisper: {ex.Message}", nameof(TwitchBot<T>));
        }
    }

    public void StartingDistribution(string message)
    {
        _ = Task.Run(async () =>
        {
            if (!IsConnected)
                return;

            SendMessage("5...");
            await Task.Delay(1_000).ConfigureAwait(false);
            SendMessage("4...");
            await Task.Delay(1_000).ConfigureAwait(false);
            SendMessage("3...");
            await Task.Delay(1_000).ConfigureAwait(false);
            SendMessage("2...");
            await Task.Delay(1_000).ConfigureAwait(false);
            SendMessage("1...");
            await Task.Delay(1_000).ConfigureAwait(false);

            if (!string.IsNullOrWhiteSpace(message))
                SendMessage(message);
        });
    }

    private static int GenerateUniqueTradeID() => Interlocked.Increment(ref nextTradeId);

    private bool AddToTradeQueue(T pk, int code, OnWhisperReceivedArgs e, RequestSignificance sig, PokeRoutineType type, out string msg)
    {
        var userId = ulong.Parse(e.WhisperMessage.UserId);
        var name = e.WhisperMessage.DisplayName;

        if (client == null)
        {
            msg = $"@{name}: Twitch client is not connected. Please try again later.";
            return false;
        }

        var trainer = new PokeTradeTrainerInfo(name, userId);
        var notifier = new TwitchTradeNotifier<T>(pk, trainer, code, e.WhisperMessage.Username, client, configuredChannel ?? Settings.Channel, Hub.Config.Twitch);

        if (TradeExtensions<T>.IsItemBlocked(pk))
        {
            var itemName = pk.HeldItem > 0 ? GameInfo.GetStrings("en").Item[pk.HeldItem] : "(none)";
            msg = $"@{name}: Trade blocked — the held item '{itemName}' cannot be traded.";
            return false;
        }

        var tradeType = type == PokeRoutineType.SeedCheck ? PokeTradeType.Seed : PokeTradeType.Specific;
        var detail = new PokeTradeDetail<T>(pk, trainer, notifier, tradeType, code, sig == RequestSignificance.Favored);
        var uniqueTradeId = GenerateUniqueTradeID();
        var trade = new TradeEntry<T>(detail, userId, type, name, uniqueTradeId);
        var added = Info.AddToTradeQueue(trade, userId, sig == RequestSignificance.Owner);

        if (added == QueueResultAdd.AlreadyInQueue)
        {
            msg = $"@{name}: Sorry, you are already in the queue.";
            return false;
        }

        if (added == QueueResultAdd.NotAllowedItem)
        {
            var itemName = pk.HeldItem > 0 ? GameInfo.GetStrings("en").Item[pk.HeldItem] : "(none)";
            msg = $"@{name}: Trade blocked — the held item '{itemName}' cannot be traded in PLZA.";
            return false;
        }

        var position = Info.CheckPosition(userId, uniqueTradeId, type);
        msg = $"@{name}: Added to the {type} queue, unique ID: {detail.ID}. Current Position: {position.Position}";

        var botCount = Info.Hub.Bots.Count;
        if (position.Position > botCount)
        {
            var eta = Info.Hub.Config.Queues.EstimateDelay(position.Position, botCount);
            msg += $". Estimated: {eta:F1} minutes.";
        }

        return true;
    }

    public Task StopAsync()
    {
        Stop();
        return Task.CompletedTask;
    }

    public void Stop()
    {
        try
        {
            CleanupResources();
            LogUtil.LogInfo("Twitch Bot stopped", nameof(TwitchBot<T>));
        }
        catch (Exception ex)
        {
            LogUtil.LogError($"Error stopping Twitch Bot: {ex.Message}", nameof(TwitchBot<T>));
        }
    }

    public void Dispose()
    {
        try
        {
            CleanupResources();
        }
        catch (Exception ex)
        {
            LogUtil.LogError($"Error disposing Twitch Bot: {ex.Message}", nameof(TwitchBot<T>));
        }
    }

    private void CleanupResources()
    {
        isConnected = false;
        CleanupClientOnly();

        if (echoForwarder != null)
        {
            EchoUtil.Forwarders.Remove(echoForwarder);
            echoForwarder = null;
        }

        userCommandCleanupTimer?.Stop();
        userCommandCleanupTimer?.Dispose();
        userCommandCleanupTimer = null;

        lock (UserCommandLock)
            UserLastCommand.Clear();
    }

    private void CleanupClientOnly()
    {
        if (client == null)
            return;

        UnsubscribeEventHandlers();

        try
        {
            if (client.IsConnected)
                client.Disconnect();
        }
        catch (Exception ex)
        {
            LogUtil.LogError($"Error disconnecting old Twitch client: {ex.Message}", nameof(TwitchBot<T>));
        }
        finally
        {
            client = null;
        }
    }

    private void UnsubscribeEventHandlers()
    {
        if (client == null)
            return;

        try
        {
            client.OnLog -= OnLog;
            client.OnJoinedChannel -= OnJoinedChannel;
            client.OnMessageReceived -= OnMessageReceived;
            client.OnWhisperReceived -= OnWhisperReceived;
            client.OnChatCommandReceived -= OnChatCommandReceived;
            client.OnWhisperCommandReceived -= OnWhisperCommandReceived;
            client.OnConnected -= OnConnected;
            client.OnIncorrectLogin -= OnIncorrectLogin;
            client.OnConnectionError -= OnConnectionError;
            client.OnDisconnected -= OnDisconnected;
            client.OnFailureToReceiveJoinConfirmation -= OnFailureToReceiveJoinConfirmation;
            client.OnLeftChannel -= OnLeftChannel;

            if (logMessageSent != null)
            {
                client.OnMessageSent -= logMessageSent;
                logMessageSent = null;
            }

            if (logWhisperSent != null)
            {
                client.OnWhisperSent -= logWhisperSent;
                logWhisperSent = null;
            }

            if (logMessageThrottled != null)
            {
                client.OnMessageThrottled -= logMessageThrottled;
                logMessageThrottled = null;
            }

            if (logWhisperThrottled != null)
            {
                client.OnWhisperThrottled -= logWhisperThrottled;
                logWhisperThrottled = null;
            }

            if (logError != null)
            {
                client.OnError -= logError;
                logError = null;
            }
        }
        catch (Exception ex)
        {
            LogUtil.LogError($"Error unsubscribing Twitch event handlers: {ex.Message}", nameof(TwitchBot<T>));
        }
    }

    private void CleanupUserCommands(object? sender, System.Timers.ElapsedEventArgs e)
    {
        try
        {
            var cutoff = DateTime.UtcNow.AddHours(-1);

            lock (UserCommandLock)
            {
                var keysToRemove = UserLastCommand
                    .Where(x => x.Value < cutoff)
                    .Select(x => x.Key)
                    .ToList();

                foreach (var key in keysToRemove)
                    UserLastCommand.Remove(key);

                if (keysToRemove.Count > 0)
                    LogUtil.LogInfo($"Cleaned up {keysToRemove.Count} old user command entries", nameof(TwitchBot<T>));
            }
        }
        catch (Exception ex)
        {
            LogUtil.LogError($"Error during user command cleanup: {ex.Message}", nameof(TwitchBot<T>));
        }
    }

    private string HandleCommand(TwitchLibMessage m, string c, string args, bool whisper)
    {
        bool IsSudo() => m is ChatMessage chat && (chat.IsBroadcaster || Settings.IsSudo(m.Username));
        bool IsSubscriber() => m is ChatMessage { IsSubscriber: true };

        switch (c)
        {
            case "donate":
                return Settings.DonationLink.Length > 0
                    ? $"Here's the donation link! Thank you for your support :3 {Settings.DonationLink}"
                    : string.Empty;

            case "discord":
                return Settings.DiscordLink.Length > 0
                    ? $"Here's the Discord Server Link, have a nice stay :3 {Settings.DiscordLink}"
                    : string.Empty;

            case "tutorial":
            case "help":
                return $"{Settings.TutorialText} {Settings.TutorialLink}".Trim();

            case "trade":
            case "t":
                _ = TwitchCommandsHelper<T>.AddToWaitingList(args, m.DisplayName, m.Username, ulong.Parse(m.UserId), IsSubscriber(), out var tradeMessage);
                if (tradeMessage.Contains("Please read what you are supposed to type", StringComparison.OrdinalIgnoreCase) && Settings.TutorialLink.Length > 0)
                    tradeMessage += $"\nUsage Tutorial: {Settings.TutorialLink}";
                return tradeMessage;

            case "ts":
            case "queue":
            case "position":
                var userId = ulong.Parse(m.UserId);
                var tradeEntry = Info.GetDetail(userId);
                return tradeEntry != null
                    ? $"@{m.Username}: {Info.GetPositionString(userId, tradeEntry.UniqueTradeID)}"
                    : $"@{m.Username}: You are not currently in the queue.";

            case "tc":
            case "cancel":
            case "remove":
                return $"@{m.Username}: {TwitchCommandsHelper<T>.ClearTrade(ulong.Parse(m.UserId))}";

            case "code" when whisper:
                return TwitchCommandsHelper<T>.GetCode(ulong.Parse(m.UserId));

            case "tca" when !IsSudo():
            case "pr" when !IsSudo():
            case "pc" when !IsSudo():
            case "tt" when !IsSudo():
            case "tcu" when !IsSudo():
                return "This command is locked for sudo users only!";

            case "tca":
                Info.ClearAllQueues();
                return "Cleared all queues!";

            case "pr":
                return Info.Hub.Ledy.Pool.Reload(Hub.Config.Folder.DistributeFolder)
                    ? $"Reloaded from folder. Pool count: {Info.Hub.Ledy.Pool.Count}"
                    : "Failed to reload from folder.";

            case "pc":
                return $"The pool count is: {Info.Hub.Ledy.Pool.Count}";

            case "tt":
                return Info.Hub.Queues.Info.ToggleQueue()
                    ? "Users are now able to join the trade queue."
                    : "Changed queue settings: **Users CANNOT join the queue until it is turned back on.**";

            case "tcu":
                return TwitchCommandsHelper<T>.ClearTrade(args);

            default:
                return string.Empty;
        }
    }
}
