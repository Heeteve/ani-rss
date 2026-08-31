package ani.rss.util.other;

import ani.rss.entity.Ani;
import ani.rss.entity.Config;
import ani.rss.entity.NotificationConfig;
import ani.rss.enums.NotificationStatusEnum;
import ani.rss.enums.NotificationTypeEnum;
import ani.rss.notification.*;
import cn.hutool.core.thread.ExecutorBuilder;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.ReflectUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class NotificationUtil {
    private static final ExecutorService EXECUTOR_SERVICE = ExecutorBuilder.create()
            .setCorePoolSize(1)
            .setMaxPoolSize(1)
            .setWorkQueue(new LinkedBlockingQueue<>(256))
            .build();

    public final static Map<NotificationTypeEnum, Class<? extends BaseNotification>>
            NOTIFICATION_MAP =
            Map.of(
                    NotificationTypeEnum.EMBY_REFRESH, EmbyRefreshNotification.class,
                    NotificationTypeEnum.MAIL, MailNotification.class,
                    NotificationTypeEnum.SERVER_CHAN, ServerChanNotification.class,
                    NotificationTypeEnum.SYSTEM, SystemNotification.class,
                    NotificationTypeEnum.TELEGRAM, TelegramNotification.class,
                    NotificationTypeEnum.WEB_HOOK, WebHookNotification.class,
                    NotificationTypeEnum.SHELL, ShellNotification.class,
                    NotificationTypeEnum.FILE_MOVE, FileMoveNotification.class,
                    NotificationTypeEnum.OPEN_LIST_UPLOAD, OpenListUploadNotification.class,
                    NotificationTypeEnum.BARK, BarkNotification.class
            );

    /**
     * 发送通知
     *
     * @param config                 设置
     * @param ani                    订阅
     * @param text                   通知内容
     * @param notificationStatusEnum 通知状态
     * @return 是否已创建至少一个通知发送任务
     */
    public static boolean send(Config config, Ani ani, String text, NotificationStatusEnum notificationStatusEnum) {
        if (Objects.isNull(ani)) {
            log.warn("通知未发送：订阅为空，状态={}", notificationStatusEnum);
            return false;
        }

        String title = ani.getTitle();
        if (Objects.isNull(notificationStatusEnum)) {
            log.warn("通知未发送：通知状态为空，订阅={}", title);
            return false;
        }

        if (!Boolean.TRUE.equals(ani.getMessage())) {
            // 未开启此订阅通知
            log.info("通知跳过：订阅未开启消息通知，订阅={} 状态={}", title, notificationStatusEnum);
            return false;
        }

        if (Objects.isNull(config)) {
            log.warn("通知未发送：全局配置为空，订阅={} 状态={}", title, notificationStatusEnum);
            return false;
        }

        List<NotificationConfig> notificationConfigList = config.getNotificationConfigList();
        if (Objects.isNull(notificationConfigList) || notificationConfigList.isEmpty()) {
            log.warn("通知未发送：未配置通知渠道，订阅={} 状态={}", title, notificationStatusEnum);
            return false;
        }

        log.info("通知开始分发：订阅={} 状态={} 渠道数={} 内容={}",
                title, notificationStatusEnum, notificationConfigList.size(), text);

        notificationConfigList = notificationConfigList
                .stream()
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingLong(it -> Objects.requireNonNullElse(it.getSort(), Long.MAX_VALUE)))
                .toList();

        int queuedCount = 0;
        for (int index = 0; index < notificationConfigList.size(); index++) {
            NotificationConfig notificationConfig = notificationConfigList.get(index);
            boolean enable = Boolean.TRUE.equals(notificationConfig.getEnable());
            Integer retryValue = notificationConfig.getRetry();
            int retry = Objects.isNull(retryValue) ? 0 : Math.max(retryValue, 0);
            NotificationTypeEnum notificationType = notificationConfig.getNotificationType();
            List<NotificationStatusEnum> statusList = notificationConfig.getStatusList();

            // 通知状态可能被删除
            statusList = Objects.isNull(statusList)
                    ? List.of()
                    : statusList.stream().filter(Objects::nonNull).toList();

            if (!enable) {
                // 未开启
                log.info("通知跳过：渠道#{} 未启用，订阅={} 状态={}", index + 1, title, notificationStatusEnum);
                continue;
            }

            boolean enabledStatus = statusList.contains(notificationStatusEnum);
            if (notificationStatusEnum == NotificationStatusEnum.RSS_UPDATE &&
                    statusList.contains(NotificationStatusEnum.DOWNLOAD_START)) {
                // 兼容旧通知配置，原有“开始下载”通知也接收 RSS 更新
                enabledStatus = true;
                log.info("通知兼容路由：渠道#{} 使用“开始下载”配置接收 RSS 更新，订阅={}", index + 1, title);
            }

            if (!enabledStatus) {
                // 未启用 通知状态
                log.info("通知跳过：渠道#{} 未启用状态 {}，订阅={} 已启用状态={}",
                        index + 1, notificationStatusEnum, title, statusList);
                continue;
            }

            if (Objects.isNull(notificationType)) {
                // 通知类型可能已经被删除
                log.warn("通知跳过：渠道#{} 通知类型为空，订阅={} 状态={}", index + 1, title, notificationStatusEnum);
                continue;
            }

            if (!NOTIFICATION_MAP.containsKey(notificationType)) {
                log.warn("通知跳过：渠道#{} 不支持通知类型 {}，订阅={} 状态={}",
                        index + 1, notificationType, title, notificationStatusEnum);
                continue;
            }

            Class<? extends BaseNotification> aClass = NOTIFICATION_MAP.get(notificationType);
            int channelIndex = index + 1;

            try {
                BaseNotification baseNotification = ReflectUtil.newInstance(aClass);
                EXECUTOR_SERVICE.execute(() -> sendNotification(
                        baseNotification,
                        notificationConfig,
                        ani,
                        text,
                        notificationStatusEnum,
                        retry,
                        channelIndex
                ));
                queuedCount++;
                log.info("通知已入队：渠道#{} 类型={} 订阅={} 状态={} 重试次数={}",
                        channelIndex, notificationType, title, notificationStatusEnum, retry);
            } catch (Exception e) {
                log.error("通知入队失败：渠道#{} 类型={} 订阅={} 状态={}",
                        channelIndex, notificationType, title, notificationStatusEnum, e);
            }
        }

        if (queuedCount == 0) {
            log.warn("通知未发送：没有符合条件的通知渠道，订阅={} 状态={}", title, notificationStatusEnum);
            return false;
        }
        return true;
    }

    /**
     * 发送单个渠道通知
     *
     * @param baseNotification       通知实现
     * @param notificationConfig     通知配置
     * @param ani                    订阅
     * @param text                   通知内容
     * @param notificationStatusEnum 通知状态
     * @param retry                  重试次数
     * @param channelIndex           渠道序号
     */
    private static void sendNotification(BaseNotification baseNotification,
                                         NotificationConfig notificationConfig,
                                         Ani ani,
                                         String text,
                                         NotificationStatusEnum notificationStatusEnum,
                                         int retry,
                                         int channelIndex) {
        String title = ani.getTitle();
        NotificationTypeEnum notificationType = notificationConfig.getNotificationType();
        int totalAttempts = Math.max(retry, 1);

        for (int currentAttempt = 1; currentAttempt <= totalAttempts; currentAttempt++) {
            try {
                Boolean sent = baseNotification.send(notificationConfig, ani, text, notificationStatusEnum);
                if (Boolean.TRUE.equals(sent)) {
                    log.info("通知发送成功：渠道#{} 类型={} 订阅={} 状态={} 第{}/{}次",
                            channelIndex, notificationType, title, notificationStatusEnum,
                            currentAttempt, totalAttempts);
                    return;
                }
                log.warn("通知发送失败：渠道#{} 类型={} 订阅={} 状态={} 第{}/{}次，渠道返回失败结果",
                        channelIndex, notificationType, title, notificationStatusEnum,
                        currentAttempt, totalAttempts);
            } catch (Exception e) {
                log.error("通知发送异常：渠道#{} 类型={} 订阅={} 状态={} 第{}/{}次",
                        channelIndex, notificationType, title, notificationStatusEnum,
                        currentAttempt, totalAttempts, e);
            }

            if (currentAttempt < totalAttempts) {
                ThreadUtil.sleep(1000);
            }
        }

        log.error("通知最终失败：渠道#{} 类型={} 订阅={} 状态={} 已尝试{}次",
                channelIndex, notificationType, title, notificationStatusEnum, totalAttempts);
    }
}
