package driver

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindDevice_BySysfsWhenUdevLinkIsAbsent(t *testing.T) {
	// by-id пустой — ровно случай образа без udev-правил.
	byIDDir = t.TempDir()
	sys := t.TempDir()
	sysBlockDir = sys
	for name, serial := range map[string]string{
		"vda": "0000000000000000root",
		"vdb": "a91bd4aa8c9b420fbb44",
	} {
		if err := os.MkdirAll(filepath.Join(sys, name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(sys, name, "serial"), []byte(serial+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Устройство без серийника не должно ломать обход.
	if err := os.MkdirAll(filepath.Join(sys, "loop0"), 0o755); err != nil {
		t.Fatal(err)
	}

	got, err := FindDevice("a91bd4aa8c9b420fbb44")
	if err != nil {
		t.Fatalf("FindDevice: %v", err)
	}
	if got != "/dev/vdb" {
		t.Errorf("устройство = %q, ожидалось /dev/vdb", got)
	}
}

// Не нашли — ошибка, а не «возьмём похожее». Примонтировать чужое устройство
// хуже, чем не примонтировать ничего.
func TestFindDevice_UnknownSerialIsAnError(t *testing.T) {
	byIDDir = t.TempDir()
	sys := t.TempDir()
	sysBlockDir = sys
	if err := os.MkdirAll(filepath.Join(sys, "vda"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sys, "vda", "serial"), []byte("другой\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if got, err := FindDevice("a91bd4aa8c9b420fbb44"); err == nil {
		t.Fatalf("несовпадающий серийник дал устройство %q вместо ошибки", got)
	}
}

func TestFindDevice_EmptySerialIsAnError(t *testing.T) {
	if _, err := FindDevice(""); err == nil {
		t.Fatal("пустой серийник должен быть ошибкой")
	}
}

// Серийник обрезается до предела udev: платформа даёт ровно 20 символов, но
// вызывающий может передать полный id, и искать надо всё равно по обрезанному.
func TestFindDevice_SerialTruncatedToUdevLimit(t *testing.T) {
	byIDDir = t.TempDir()
	sys := t.TempDir()
	sysBlockDir = sys
	if err := os.MkdirAll(filepath.Join(sys, "vdb"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sys, "vdb", "serial"),
		[]byte("a91bd4aa8c9b420fbb44\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := FindDevice("a91bd4aa8c9b420fbb44544284c9321a") // полный hex
	if err != nil {
		t.Fatalf("FindDevice: %v", err)
	}
	if got != "/dev/vdb" {
		t.Errorf("устройство = %q, ожидалось /dev/vdb", got)
	}
}
