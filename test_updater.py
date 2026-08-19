# test_updater.py — tests สำหรับ logic เทียบเวอร์ชันของระบบอัปเดต
# (ไม่ยิง network จริง — mock GitHub API)
import json
import io

import pytest

import updater as U


class TestParseVersion:
    def test_plain(self):
        assert U.parse_version("13.0.0") == (13, 0, 0)

    def test_with_v_prefix(self):
        assert U.parse_version("v13.2.5") == (13, 2, 5)

    def test_partial(self):
        assert U.parse_version("13.1") == (13, 1)

    def test_suffix_stripped(self):
        assert U.parse_version("13.0.0-beta") == (13, 0, 0)
        assert U.parse_version("v13.0.1+build7") == (13, 0, 1)

    def test_garbage(self):
        assert U.parse_version("") == (0,)
        assert U.parse_version("abc") == (0,)


class TestIsNewer:
    def test_patch_bump(self):
        assert U.is_newer("13.0.1", "13.0.0") is True

    def test_minor_bump(self):
        assert U.is_newer("13.1.0", "13.0.9") is True

    def test_major_bump(self):
        assert U.is_newer("14.0.0", "13.9.9") is True

    def test_same(self):
        assert U.is_newer("13.0.0", "13.0.0") is False

    def test_older(self):
        assert U.is_newer("12.9.9", "13.0.0") is False

    def test_v_prefix_both(self):
        assert U.is_newer("v13.0.1", "v13.0.0") is True


class TestCheckForUpdate:
    def _fake_release(self, tag, assets):
        return json.dumps({
            "tag_name": tag,
            "name": tag,
            "body": "release notes",
            "html_url": "https://github.com/x/y/releases/tag/" + tag,
            "assets": assets,
        }).encode("utf-8")

    def _patch_urlopen(self, monkeypatch, payload):
        class FakeResp:
            def __init__(self, data): self._d = data
            def read(self): return self._d
            def __enter__(self): return self
            def __exit__(self, *a): return False
        monkeypatch.setattr(U.urllib.request, "urlopen",
                            lambda req, timeout=0: FakeResp(payload))

    def test_returns_info_when_newer(self, monkeypatch):
        payload = self._fake_release("v99.0.0", [
            {"name": "LaserControl-v99.0.0.zip",
             "browser_download_url": "https://x/LaserControl.zip"},
        ])
        self._patch_urlopen(monkeypatch, payload)
        info = U.check_for_update()
        assert info is not None
        assert info.version == "v99.0.0"
        assert info.download_url.endswith(".zip")

    def test_returns_none_when_same_version(self, monkeypatch):
        payload = self._fake_release("v" + U.CURRENT_VERSION, [
            {"name": "LaserControl.zip", "browser_download_url": "https://x/a.zip"},
        ])
        self._patch_urlopen(monkeypatch, payload)
        assert U.check_for_update() is None

    def test_raises_when_newer_but_no_zip_asset(self, monkeypatch):
        payload = self._fake_release("v99.0.0", [
            {"name": "notes.txt", "browser_download_url": "https://x/notes.txt"},
        ])
        self._patch_urlopen(monkeypatch, payload)
        with pytest.raises(U.UpdateCheckError):
            U.check_for_update()

    def test_raises_on_network_error(self, monkeypatch):
        def boom(req, timeout=0):
            raise U.urllib.error.URLError("no net")
        monkeypatch.setattr(U.urllib.request, "urlopen", boom)
        with pytest.raises(U.UpdateCheckError):
            U.check_for_update()

    def test_returns_none_on_404_no_releases(self, monkeypatch):
        def http404(req, timeout=0):
            raise U.urllib.error.HTTPError("url", 404, "Not Found", {}, None)
        monkeypatch.setattr(U.urllib.request, "urlopen", http404)
        # ไม่มี release เลย = ถือว่าเป็นล่าสุด (ไม่ raise)
        assert U.check_for_update() is None

    def test_async_reports_error_separately(self, monkeypatch):
        import threading as _t
        def boom(req, timeout=0):
            raise U.urllib.error.URLError("no net")
        monkeypatch.setattr(U.urllib.request, "urlopen", boom)
        done = _t.Event()
        box = {}
        def on_result(info, err=None):
            box["info"], box["err"] = info, err
            done.set()
        U.check_for_update_async(on_result, timeout=1)
        assert done.wait(3.0)
        assert box["info"] is None
        assert box["err"]  # มีข้อความ error


class TestApplyGuardedWhenNotFrozen:
    def test_apply_raises_when_not_frozen(self):
        # รันจาก source → apply ต้องไม่ทำงาน
        info = U.UpdateInfo("v99.0.0", "https://x/a.zip", "", "")
        with pytest.raises(RuntimeError):
            U.apply_update(info)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
