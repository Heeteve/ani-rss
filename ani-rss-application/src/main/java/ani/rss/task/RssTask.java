package ani.rss.task;

import ani.rss.commons.ExceptionUtils;
import ani.rss.entity.Ani;
import ani.rss.entity.Config;
import ani.rss.service.DownloadService;
import ani.rss.util.other.AniUtil;
import ani.rss.util.other.ConfigUtil;
import ani.rss.util.other.TorrentUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.extra.spring.SpringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * RSS
 */
@Slf4j
@Component
public class RssTask implements BaseTask {
    public static final List<Ani> ANI_LIST = AniUtil.ANI_LIST;
    public static final AtomicBoolean DOWNLOAD_LOCK = new AtomicBoolean(false);
    public static AtomicBoolean LOOP = new AtomicBoolean(false);

    public static void syncDownload() {
        syncDownload(ANI_LIST);
    }

    public static void syncDownload(List<Ani> aniList) {
        syncLock(lock -> lock.set(true));
        try {
            download(aniList);
        } catch (Exception e) {
            String message = ExceptionUtils.getMessage(e);
            log.error(message, e);
        } finally {
            DOWNLOAD_LOCK.set(false);
        }
    }

    public static void syncLock() {
        syncLock(null);
    }

    public static void syncLock(Consumer<AtomicBoolean> consumer) {
        synchronized (DOWNLOAD_LOCK) {
            if (DOWNLOAD_LOCK.get()) {
                throw new IllegalStateException("存在未完成任务，请等待...");
            }
            if (Objects.nonNull(consumer)) {
                consumer.accept(DOWNLOAD_LOCK);
            }
        }
    }

    public static void download(List<Ani> aniList) {
        DownloadService downloadService = SpringUtil.getBean(DownloadService.class);
        boolean downloadLogin = false;
        boolean downloadLoginFailed = false;
        for (Ani ani : aniList) {
            if (!LOOP.get()) {
                // 停止循环
                log.warn("RSS 刷新已停止，本轮未继续处理剩余订阅");
                return;
            }

            if (!ANI_LIST.contains(ani)) {
                // 订阅可能已经被删除
                continue;
            }

            String title = ani.getTitle();
            Boolean enable = ani.getEnable();
            if (!enable) {
                log.debug("{} 未启用", title);
                continue;
            }

            try {
                if (!Boolean.TRUE.equals(ani.getRssNotificationOnly())) {
                    if (downloadLoginFailed) {
                        continue;
                    }
                    if (!downloadLogin) {
                        downloadLogin = TorrentUtil.login();
                        if (!downloadLogin) {
                            downloadLoginFailed = true;
                            continue;
                        }
                    }
                } else {
                    log.info("RSS 刷新：订阅={} 使用仅通知模式，跳过下载器登录", title);
                }
                downloadService.downloadAni(ani);
            } catch (Exception e) {
                String message = ExceptionUtils.getMessage(e);
                log.error("{} {}", title, message);
                log.error(message, e);
            }
            // 避免短时间频繁请求导致流控
            ThreadUtil.sleep(500);
        }
    }

    @Override
    public void accept(AtomicBoolean loop) {
        Config config = ConfigUtil.CONFIG;
        Integer sleep = config.getRssSleepMinutes();

        if (!config.getRss()) {
            log.debug("rss未启用");
            ThreadUtil.sleep(sleep, TimeUnit.MINUTES);
            return;
        }

        try {
            LOOP = loop;
            syncDownload(ANI_LIST);
        } catch (Exception e) {
            String message = ExceptionUtils.getMessage(e);
            log.error(message, e);
        }
        ThreadUtil.sleep(sleep, TimeUnit.MINUTES);
    }
}
