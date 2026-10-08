import concurrent.futures
import os
import tempfile

import app.main as main


def test_concurrent_bids():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    old_path = main.DB_PATH
    main.DB_PATH = path

    try:
        main.init_db()
        amounts = [1100 + i * 10 for i in range(20)]

        def submit(amount):
            return main.place_bid_transaction(f"bidder-{amount}", amount)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(submit, amounts))

        state = main.get_state()
        accepted = [result for result in results if result[0]]

        assert len(accepted) >= 1
        assert state["highest_bid"] == max(amounts)
        assert state["highest_bidder"] == f"bidder-{max(amounts)}"

    finally:
        main.DB_PATH = old_path
        try:
            os.remove(path)
        except OSError:
            pass
