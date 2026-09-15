package driver

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// byIDDir — где udev раскладывает симлинки by-id. Переменная, а не константа,
// ради тестов.
var byIDDir = "/dev/disk/by-id"

// sysBlockDir — корень блочных устройств в sysfs.
var sysBlockDir = "/sys/block"

// serialLimit — сколько символов серийника udev кладёт в имя by-id. Замерено
// на cluster1 21.08: платформа даёт ровно столько, и обрезать длиннее нечего.
const serialLimit = 20

// FindDevice ищет блочное устройство тома по серийнику.
//
// Два пути, и второй не роскошь. Основной — симлинк
// /dev/disk/by-id/virtio-<serial>, который создаёт udev: так это делают все
// облачные CSI, и на пробе 21.08 он появлялся через ~2 с после горячего
// подключения. Но by-id существует ровно постольку, поскольку в системе жив
// udev с нужными правилами — на минимальном образе его может не быть вовсе.
// Поэтому запасной путь читает серийник прямо из sysfs
// (/sys/block/vd*/serial), не завися ни от чего, кроме ядра.
//
// Совпадение серийника — единственная связь между томом платформы и
// устройством на ноде. Не нашли — возвращаем ошибку и НЕ пытаемся угадать по
// порядку появления или по размеру: примонтировать «похожее» устройство хуже,
// чем не примонтировать ничего.
func FindDevice(serial string) (string, error) {
	if serial == "" {
		return "", fmt.Errorf("пустой серийник тома")
	}
	if len(serial) > serialLimit {
		serial = serial[:serialLimit]
	}

	link := filepath.Join(byIDDir, "virtio-"+serial)
	if resolved, err := filepath.EvalSymlinks(link); err == nil {
		return resolved, nil
	}

	dev, err := findBySysfs(serial)
	if err != nil {
		return "", err
	}
	if dev == "" {
		return "", fmt.Errorf("устройство с серийником %s не найдено ни в %s, ни в %s",
			serial, byIDDir, sysBlockDir)
	}
	return dev, nil
}

// findBySysfs обходит /sys/block/*/serial. Возвращает "" если совпадения нет.
func findBySysfs(serial string) (string, error) {
	entries, err := os.ReadDir(sysBlockDir)
	if err != nil {
		return "", fmt.Errorf("чтение %s: %w", sysBlockDir, err)
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(sysBlockDir, e.Name(), "serial"))
		if err != nil {
			continue // не у всякого устройства есть серийник — это не ошибка
		}
		if strings.TrimSpace(string(raw)) == serial {
			return filepath.Join("/dev", e.Name()), nil
		}
	}
	return "", nil
}
